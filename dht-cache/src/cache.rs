//! Cached access to the DHT

mod local;

use std::sync::Arc;
use std::{collections::BTreeMap, time::Duration};

use futures_util::{Stream, StreamExt};
use libp2p::Swarm;
use serde_json::Value;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::RwLock;
use tokio::time;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::domolibp2p::{self, generate_rsa_key};
use crate::{
    cache::local::DomoCacheStateMessage,
    dht::{dht_channel, Command, Event as DhtEvent},
    domolibp2p::DomoBehaviour,
    utils, Error,
};

use self::local::{DomoCacheElement, LocalCache, Query};

/// DHT state change
#[derive(Debug)]
pub enum Event {
    /// Persistent, structured data
    ///
    /// The information is persisted across nodes.
    /// Newly joining nodes will receive it from other participants and
    /// the local cache can be queried for it.
    PersistentData(DomoCacheElement),
    /// Volatile, unstructured data
    ///
    /// The information is transmitted across all the nodes participating
    VolatileData(Value),
    /// Notify the peer availability
    ReadyPeers(Vec<String>),
}

/// Builder for a Cached DHT Node
// TODO: make it Clone
pub struct Builder {
    cfg: crate::Config,
}

impl Builder {
    /// Create a new Builder from a [crate::Config]
    pub fn from_config(cfg: crate::Config) -> Builder {
        Builder { cfg }
    }

    /// Instantiate a new DHT node a return
    pub async fn make_channel(self) -> Result<(Cache, impl Stream<Item = Event>), crate::Error> {
        let loopback_only = self.cfg.loopback;
        let shared_key = domolibp2p::parse_hex_key(&self.cfg.shared_key)?;
        let private_key_file = self.cfg.private_key.as_ref();

        // Create a random local key.
        let mut pkcs8_der = if let Some(pk_path) = private_key_file {
            match std::fs::read(pk_path) {
                Ok(pem) => {
                    let der = pem_rfc7468::decode_vec(&pem)?;
                    der.1
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // Generate a new key and put it into the file at the given path
                    let (pem, der) = generate_rsa_key();
                    std::fs::write(pk_path, pem)?;
                    der
                }
                Err(e) => return Err(e.into()),
            }
        } else {
            generate_rsa_key().1
        };

        let local_key_pair = crate::Keypair::rsa_from_pkcs8(&mut pkcs8_der)?;
        let swarm = domolibp2p::start(shared_key, local_key_pair, loopback_only).await?;

        let local = LocalCache::with_config(&self.cfg).await;
        // TODO: add a configuration item for the resend interval
        Ok(cache_channel(local, swarm, 1000))
    }
}

/// Cached DHT
///
/// It keeps a local cache of the dht state and allow to query the persistent topics
pub struct Cache {
    peer_id: String,
    local: LocalCache,
    peers: Arc<RwLock<PeersState>>,
    cmd: UnboundedSender<Command>,
}

/// Information regarding the known peers
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord)]
pub struct PeerInfo {
    /// libp2p Identifier
    pub peer_id: String,
    /// Hash of its cache of the DHT
    pub hash: u64,
    /// Last time the peer updated its state
    ///
    /// TODO: use a better type
    pub last_seen: u128,
}

impl Cache {
    /// Send a volatile message
    ///
    /// Volatile messages are unstructured and do not persist in the DHT.
    pub fn send(&self, value: Value) -> Result<(), Error> {
        self.cmd
            .send(Command::Broadcast(value.to_owned()))
            .map_err(|_| Error::Channel)?;

        Ok(())
    }

    /// Persist a value within the DHT
    ///
    /// It is identified by the topic and uuid value
    pub async fn put(
        &self,
        topic: impl Into<String>,
        uuid: impl Into<String>,
        value: Value,
    ) -> Result<(), Error> {
        let elem = DomoCacheElement {
            topic_name: topic.into(),
            topic_uuid: uuid.into(),
            value,
            publication_timestamp: utils::get_epoch_ms(),
            publisher_peer_id: self.peer_id.clone(),
            ..Default::default()
        };

        self.local.put(&elem).await;

        self.cmd
            .send(Command::Publish(serde_json::to_value(&elem)?))
            .map_err(|_| Error::Channel)?;

        Ok(())
    }

    /// Delete a value within the DHT
    ///
    /// It inserts the deletion entry and the entry value will be marked as deleted and removed
    /// from the stored cache.
    pub async fn del(
        &self,
        topic: impl Into<String>,
        uuid: impl Into<String>,
    ) -> Result<(), Error> {
        let elem = DomoCacheElement {
            topic_name: topic.into(),
            topic_uuid: uuid.into(),
            publication_timestamp: utils::get_epoch_ms(),
            publisher_peer_id: self.peer_id.clone(),
            deleted: true,
            ..Default::default()
        };

        self.local.put(&elem).await;

        self.cmd
            .send(Command::Publish(serde_json::to_value(&elem)?))
            .map_err(|_| Error::Channel)?;

        Ok(())
    }

    /// Query the local cache
    pub fn query(&self, topic: &str) -> Query {
        self.local.query(topic)
    }

    /// Get a list of the current peers
    pub async fn peers(&self) -> Vec<PeerInfo> {
        let peers = self.peers.read().await;
        peers
            .list
            .values()
            .map(|p| PeerInfo {
                peer_id: p.peer_id.to_owned(),
                hash: p.cache_hash,
                last_seen: p.publication_timestamp,
            })
            .collect()
    }

    /// Return the current cache hash
    pub async fn get_hash(&self) -> u64 {
        self.local.get_hash().await
    }
}

#[derive(Default, Debug, Clone)]
pub(crate) struct PeersState {
    list: BTreeMap<String, DomoCacheStateMessage>,
    last_repub_timestamp: u128,
    repub_interval: u128,
}

#[derive(Debug)]
enum CacheState {
    Synced,
    Desynced { is_leader: bool },
}

impl PeersState {
    fn with_interval(repub_interval: u128) -> Self {
        Self {
            repub_interval,
            ..Default::default()
        }
    }

    fn insert(&mut self, state: DomoCacheStateMessage) {
        self.list.insert(state.peer_id.to_string(), state);
    }

    fn is_synchronized(&self, peer_id: &str, hash: u64) -> CacheState {
        let cur_ts = utils::get_epoch_ms() - self.repub_interval;
        let desync = self
            .list
            .values()
            .any(|data| data.cache_hash != hash && data.publication_timestamp > cur_ts);

        if desync {
            CacheState::Desynced {
                is_leader: !self.list.values().any(|data| {
                    data.cache_hash == hash
                        && data.peer_id.as_str() < peer_id
                        && data.publication_timestamp > cur_ts
                }),
            }
        } else {
            CacheState::Synced
        }
    }
}

/// Join the dht and keep a local cache up to date
///
/// the resend interval is expressed in milliseconds
pub fn cache_channel(
    local: LocalCache,
    swarm: Swarm<DomoBehaviour>,
    resend_interval: u64,
) -> (Cache, impl Stream<Item = Event>) {
    let local_peer_id = swarm.local_peer_id().to_string();

    let (cmd, r, _j) = dht_channel(swarm);

    let peers_state = Arc::new(RwLock::new(PeersState::with_interval(
        resend_interval as u128,
    )));

    let cache = Cache {
        peers: peers_state.clone(),
        local: local.clone(),
        cmd: cmd.clone(),
        peer_id: local_peer_id.clone(),
    };

    let stream = UnboundedReceiverStream::new(r);

    let local_read = local.clone();
    let cmd_update = cmd.clone();
    let peer_id = local_peer_id.clone();

    tokio::task::spawn(async move {
        let mut interval = time::interval(Duration::from_millis(resend_interval.max(100)));
        while !cmd_update.is_closed() {
            interval.tick().await;
            let hash = local_read.get_hash().await;
            let m = DomoCacheStateMessage {
                peer_id: peer_id.clone(),
                cache_hash: hash,
                publication_timestamp: utils::get_epoch_ms(),
            };

            if cmd_update
                .send(Command::Config(serde_json::to_value(&m).unwrap()))
                .is_err()
            {
                break;
            }
        }
    });

    // TODO: refactor once async closures are stable
    let events = stream.filter_map(move |ev| {
        let local_write = local.clone();
        let peers_state = peers_state.clone();
        let peer_id = local_peer_id.clone();
        let cmd = cmd.clone();
        async move {
            match ev {
                DhtEvent::Config(cfg) => {
                    let m: DomoCacheStateMessage = serde_json::from_str(&cfg).unwrap();

                    let hash = local_write.get_hash().await;

                    let republish = {
                        let mut peers_state = peers_state.write().await;

                        // update the peers_caches_state
                        peers_state.insert(m);

                        // check for desync
                        let sync_info = peers_state.is_synchronized(&peer_id, hash);

                        log::debug!("local {peer_id:?} {sync_info:?}  -> {peers_state:#?}");

                        if let CacheState::Desynced { is_leader } = sync_info {
                            is_leader
                                && utils::get_epoch_ms() - peers_state.last_repub_timestamp
                                    >= peers_state.repub_interval
                        } else {
                            false
                        }
                    };

                    // republish the local cache if needed
                    if republish {
                        local_write
                            .read_owned()
                            .await
                            .mem
                            .values()
                            .flat_map(|topic| topic.values())
                            .for_each(|elem| {
                                let mut elem = elem.to_owned();
                                log::debug!("resending {}", elem.topic_uuid);
                                elem.republication_timestamp = utils::get_epoch_ms();

                                // This cannot fail because `cmd` is the sender part of the
                                // `stream` we are currently reading. In practice, we are
                                // queueing the commands in order to read them later.
                                cmd.send(Command::Publish(serde_json::to_value(&elem).unwrap()))
                                    .unwrap();
                            });
                        peers_state.write().await.last_repub_timestamp = utils::get_epoch_ms();
                    }

                    None
                }
                DhtEvent::Discovered(_who) => None /* Some(DomoEvent::NewPeers(
                    who.into_iter().map(|w| w.to_string()).collect(),
                ))*/,
                DhtEvent::VolatileData(data) => {
                    // TODO we swallow errors quietly here
                    serde_json::from_str(&data)
                        .ok()
                        .map(Event::VolatileData)
                }
                DhtEvent::PersistentData(data) => {
                    if let Ok(mut elem) = serde_json::from_str::<DomoCacheElement>(&data) {
                        if elem.republication_timestamp != 0 {
                            log::debug!("Retransmission");
                        }
                        // TODO: do something with this value instead
                        elem.republication_timestamp = 0;
                        local_write
                            .try_put(&elem)
                            .await
                            .ok()
                            .map(|_| Event::PersistentData(elem))
                    } else {
                        None
                    }
                }
                DhtEvent::Ready(peers) => {
                    if !peers.is_empty() {
                        Some(Event::ReadyPeers(
                            peers.into_iter().map(|p| p.to_string()).collect()))
                    } else {
                        None
                    }
                }
            }
        }
    });

    (cache, events)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::dht::test::*;
    use futures_concurrency::prelude::*;
    use std::{collections::HashSet, pin::pin};

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn builder() {
        let cfg = crate::Config {
            shared_key: "d061545647652562b4648f52e8373b3a417fc0df56c332154460da1801b341e9"
                .to_owned(),
            ..Default::default()
        };

        let (_cache, _events) = Builder::from_config(cfg).make_channel().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn syncronization() {
        let [mut a, mut b, mut c] = make_peers(2).await;
        let mut d = make_peer(2).await;

        connect_peer(&mut a, &mut d).await;
        connect_peer(&mut b, &mut d).await;
        connect_peer(&mut c, &mut d).await;

        let a_local_cache = LocalCache::new();
        let b_local_cache = LocalCache::new();
        let c_local_cache = LocalCache::new();

        let mut expected: HashSet<_> = (0..10)
            .into_iter()
            .map(|uuid| format!("uuid-{uuid}"))
            .collect();

        let (a_c, a_ev) = cache_channel(a_local_cache, a, 5000);
        let (b_c, b_ev) = cache_channel(b_local_cache, b, 5000);
        let (c_c, c_ev) = cache_channel(c_local_cache, c, 5000);

        let mut expected_peers = HashSet::new();
        expected_peers.insert(a_c.peer_id.clone());
        expected_peers.insert(b_c.peer_id.clone());
        expected_peers.insert(c_c.peer_id.clone());

        let mut a_ev = pin!(a_ev);
        let b_ev = pin!(b_ev);
        let c_ev = pin!(c_ev);

        while let Some(ev) = a_ev.next().await {
            match ev {
                Event::ReadyPeers(peers) => {
                    log::info!("Ready peers {peers:?}");
                    break;
                }
                _ => log::debug!("waiting for ready {ev:?}"),
            }
        }

        for uuid in 0..10 {
            let _ = a_c
                .put(
                    "Topic",
                    &format!("uuid-{uuid}"),
                    serde_json::json!({"key": uuid}),
                )
                .await;
        }
        let mut s = (
            a_ev.map(|ev| ("a", ev)),
            b_ev.map(|ev| ("b", ev)),
            c_ev.map(|ev| ("c", ev)),
        )
            .merge();

        while !expected.is_empty() {
            let (node, ev) = s.next().await.unwrap();
            match ev {
                Event::PersistentData(data) => {
                    log::debug!("{node}: Got data {data:?}");
                    if node == "c" {
                        assert!(expected.remove(&data.topic_uuid));
                    }
                }
                _ => {
                    log::debug!("{node}: Other {ev:?}");
                }
            }
        }

        // c_c must had seen at least one of the expected peers
        let peers: HashSet<_> = c_c.peers().await.into_iter().map(|p| p.peer_id).collect();

        log::info!("peers {peers:?}");

        assert!(peers.is_subset(&expected_peers));
    }
}
