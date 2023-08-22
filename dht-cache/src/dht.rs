//! DHT Abstraction
//!

use std::time::Duration;

use crate::domolibp2p::{DomoBehaviour, OutEvent};
use futures::prelude::*;
use libp2p::{gossipsub::IdentTopic as Topic, swarm::SwarmEvent, Swarm};
use libp2p::{mdns, PeerId};
use serde_json::Value;
use time::OffsetDateTime;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;

/// Network commands
#[derive(Debug)]
pub enum Command {
    Config(Value),
    Broadcast(Value),
    Publish(Value),
    Stop,
}

/// Network Events
#[derive(Debug)]
pub enum Event {
    PersistentData(String),
    VolatileData(String),
    Config(String),
    Discovered(Vec<PeerId>),
    Ready(Vec<PeerId>),
}

fn handle_command(swarm: &mut Swarm<DomoBehaviour>, cmd: Command) -> bool {
    use Command::*;
    match cmd {
        Broadcast(val) => {
            let topic = Topic::new("domo-volatile-data");
            let m = serde_json::to_string(&val).unwrap();

            if let Err(e) = swarm.behaviour_mut().gossipsub.publish(topic, m.as_bytes()) {
                log::info!("Boradcast error: {e:?}");
            }
            true
        }
        Publish(val) => {
            let topic = Topic::new("domo-persistent-data");
            let m2 = serde_json::to_string(&val).unwrap();

            if let Err(e) = swarm
                .behaviour_mut()
                .gossipsub
                .publish(topic, m2.as_bytes())
            {
                log::info!("Publish error: {e:?}");
            }
            true
        }
        Config(val) => {
            let topic = Topic::new("domo-config");
            let m = serde_json::to_string(&val).unwrap();
            if let Err(e) = swarm.behaviour_mut().gossipsub.publish(topic, m.as_bytes()) {
                log::info!("Config error: {e:?}");
            }
            true
        }
        Stop => false,
    }
}

fn handle_swarm_event<E>(
    swarm: &mut Swarm<DomoBehaviour>,
    event: SwarmEvent<OutEvent, E>,
    ev_send: &UnboundedSender<Event>,
) -> Result<(), ()> {
    use Event::*;

    match event {
        SwarmEvent::ExpiredListenAddr { address, .. } => {
            log::info!("Address {address:?} expired");
        }
        SwarmEvent::ConnectionEstablished { .. } => {
            log::info!("Connection established ...");
        }
        SwarmEvent::ConnectionClosed { .. } => {
            log::info!("Connection closed");
        }
        SwarmEvent::ListenerError { .. } => {
            log::info!("Listener Error");
        }
        SwarmEvent::OutgoingConnectionError { .. } => {
            log::info!("Outgoing connection error");
        }
        SwarmEvent::ListenerClosed { .. } => {
            log::info!("Listener Closed");
        }
        SwarmEvent::NewListenAddr { address, .. } => {
            println!("Listening in {address:?}");
        }
        SwarmEvent::Behaviour(crate::domolibp2p::OutEvent::Gossipsub(
            libp2p::gossipsub::Event::Message {
                propagation_source: _peer_id,
                message_id: _id,
                message,
            },
        )) => {
            if let Ok(data) = String::from_utf8(message.data) {
                match message.topic.as_str() {
                    "domo-persistent-data" => {
                        ev_send.send(PersistentData(data)).map_err(|_| ())?;
                    }
                    "domo-config" => {
                        ev_send.send(Config(data)).map_err(|_| ())?;
                    }
                    "domo-volatile-data" => {
                        ev_send.send(VolatileData(data)).map_err(|_| ())?;
                    }
                    _ => {
                        log::info!("Not able to recognize message");
                    }
                }
            } else {
                log::warn!("The message does not contain utf8 data");
            }
        }
        SwarmEvent::Behaviour(crate::domolibp2p::OutEvent::Gossipsub(
            libp2p::gossipsub::Event::Subscribed { peer_id, topic },
        )) => {
            log::debug!("Peer {peer_id} subscribed to {}", topic.as_str());
        }
        SwarmEvent::Behaviour(crate::domolibp2p::OutEvent::Mdns(mdns::Event::Expired(list))) => {
            let local = OffsetDateTime::now_utc();

            for (peer, _) in list {
                log::info!("MDNS for peer {peer} expired {local:?}");
            }
        }
        SwarmEvent::Behaviour(crate::domolibp2p::OutEvent::Mdns(mdns::Event::Discovered(list))) => {
            let local = OffsetDateTime::now_utc();
            let peers = list
                .into_iter()
                .map(|(peer, _)| {
                    swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer);
                    log::info!("Discovered peer {peer} {local:?}");
                    peer
                })
                .collect();
            ev_send.send(Discovered(peers)).map_err(|_| ())?;
        }
        _ => {}
    }

    Ok(())
}

/// Spawn a new task polling constantly for new swarm Events
pub fn dht_channel(
    mut swarm: Swarm<DomoBehaviour>,
) -> (
    UnboundedSender<Command>,
    UnboundedReceiver<Event>,
    JoinHandle<Swarm<DomoBehaviour>>,
) {
    let (cmd_send, mut cmd_recv) = mpsc::unbounded_channel();
    let (ev_send, ev_recv) = mpsc::unbounded_channel();

    let handle = tokio::task::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let volatile = Topic::new("domo-volatile-data").hash();
        let persistent = Topic::new("domo-persistent-data").hash();
        let config = Topic::new("domo-config").hash();

        // Only peers that subscribed to all the topics are usable
        let check_peers = |swarm: &mut Swarm<DomoBehaviour>| {
            swarm
                .behaviour_mut()
                .gossipsub
                .all_peers()
                .filter_map(|(p, topics)| {
                    log::info!("{p}, {topics:?}");
                    (topics.contains(&&volatile)
                        && topics.contains(&&persistent)
                        && topics.contains(&&config))
                    .then(|| p.to_owned())
                })
                .collect()
        };

        loop {
            log::trace!("Looping {}", swarm.local_peer_id());
            tokio::select! {
                // the mdns event is not enough to ensure we can send messages
                _ = interval.tick() => {
                    log::debug!("{} Checking for peers", swarm.local_peer_id());
                    let peers: Vec<_> = check_peers(&mut swarm);
                        if !peers.is_empty() &&
                        ev_send.send(Event::Ready(peers)).is_err() {
                            return swarm;
                    }
                }
                cmd = cmd_recv.recv() => {
                    log::trace!("command {cmd:?}");
                    if !cmd.is_some_and(|cmd| handle_command(&mut swarm, cmd)) {
                        log::debug!("Exiting cmd");
                        return swarm
                    }
                }
                ev = swarm.select_next_some() => {
                    log::trace!("event {ev:?}");
                    if handle_swarm_event(&mut swarm, ev, &ev_send).is_err() {
                        log::debug!("Exiting ev");
                        return swarm
                    }
                }
            }
            tokio::task::yield_now().await;
        }
    });

    (cmd_send, ev_recv, handle)
}

#[cfg(test)]
pub(crate) mod test {
    use std::time::Duration;

    use super::*;
    use crate::Keypair;
    use libp2p::core::transport::MemoryTransport;
    use libp2p::core::upgrade::Version;
    use libp2p::plaintext::PlainText2Config;
    use libp2p::pnet::{PnetConfig, PreSharedKey};
    use libp2p::swarm::SwarmBuilder;
    use libp2p::yamux;
    use libp2p::Transport;
    use libp2p_swarm_test::SwarmExt;
    use serde_json::json;

    // like Swarm::new_ephemeral but with pnet variant
    fn new_ephemeral(
        behaviour_fn: impl FnOnce(Keypair) -> DomoBehaviour,
        variant: u8,
    ) -> Swarm<DomoBehaviour> {
        let identity = Keypair::generate_ed25519();
        let peer_id = PeerId::from(identity.public());
        let psk = PreSharedKey::new([variant; 32]);

        let transport = MemoryTransport::default()
            .or_transport(libp2p::tcp::async_io::Transport::default())
            .and_then(move |socket, _| PnetConfig::new(psk).handshake(socket))
            .upgrade(Version::V1)
            .authenticate(PlainText2Config {
                local_public_key: identity.public(),
            })
            .multiplex(yamux::Config::default())
            .timeout(Duration::from_secs(20))
            .boxed();

        SwarmBuilder::without_executor(transport, behaviour_fn(identity), peer_id).build()
    }

    pub async fn make_peer(variant: u8) -> Swarm<DomoBehaviour> {
        let mut swarm = new_ephemeral(|identity| DomoBehaviour::new(&identity).unwrap(), variant);
        swarm.listen().await;
        swarm
    }

    pub async fn connect_peer(a: &mut Swarm<DomoBehaviour>, b: &mut Swarm<DomoBehaviour>) {
        a.connect(b).await;

        let peers: Vec<_> = a.connected_peers().cloned().collect();

        for peer in peers {
            a.behaviour_mut().gossipsub.add_explicit_peer(&peer);
        }
    }

    pub async fn make_peers(variant: u8) -> [Swarm<DomoBehaviour>; 3] {
        let _ = env_logger::builder().is_test(true).try_init();

        let mut a = new_ephemeral(|identity| DomoBehaviour::new(&identity).unwrap(), variant);
        let mut b = new_ephemeral(|identity| DomoBehaviour::new(&identity).unwrap(), variant);
        let mut c = new_ephemeral(|identity| DomoBehaviour::new(&identity).unwrap(), variant);
        /*
        let mut a = Swarm::new_ephemeral(|identity| DomoBehaviour::new(&identity).unwrap());
        let mut b = Swarm::new_ephemeral(|identity| DomoBehaviour::new(&identity).unwrap());
        let mut c = Swarm::new_ephemeral(|identity| DomoBehaviour::new(&identity).unwrap());
        */
        for a in a.external_addresses() {
            log::info!("{a:?}");
        }

        a.listen().await;
        b.listen().await;
        c.listen().await;

        a.connect(&mut b).await;
        b.connect(&mut c).await;
        c.connect(&mut a).await;

        println!("a {}", a.local_peer_id());
        println!("b {}", b.local_peer_id());
        println!("c {}", c.local_peer_id());

        let peers: Vec<_> = a.connected_peers().cloned().collect();

        log::info!("Peers {peers:#?}");

        for peer in peers {
            a.behaviour_mut().gossipsub.add_explicit_peer(&peer);
        }

        let peers: Vec<_> = b.connected_peers().cloned().collect();

        for peer in peers {
            b.behaviour_mut().gossipsub.add_explicit_peer(&peer);
        }

        let peers: Vec<_> = c.connected_peers().cloned().collect();

        for peer in peers {
            c.behaviour_mut().gossipsub.add_explicit_peer(&peer);
        }

        [a, b, c]
    }

    #[tokio::test]
    async fn multiple_peers() {
        let [a, b, c] = make_peers(1).await;

        let (a_s, mut ar, _) = dht_channel(a);
        let (b_s, br, _) = dht_channel(b);
        let (c_s, cr, _) = dht_channel(c);

        log::info!("Waiting for peers");

        // Wait until peers are discovered
        while let Some(ev) = ar.recv().await {
            match ev {
                Event::VolatileData(data) => log::info!("volatile {data}"),
                Event::PersistentData(data) => log::info!("persistent {data}"),
                Event::Config(cfg) => log::info!("config {cfg}"),
                Event::Discovered(peers) => {
                    log::info!("found peers: {peers:?}");
                }
                Event::Ready(peers) => {
                    log::info!("ready peers: {peers:?}");
                    break;
                }
            }
        }

        let msg = json!({"a": "value"});

        a_s.send(Command::Broadcast(msg.clone())).unwrap();

        log::info!("Sent volatile");
        for r in [br, cr].iter_mut() {
            while let Some(ev) = r.recv().await {
                match ev {
                    Event::VolatileData(data) => {
                        log::info!("volatile {data}");
                        let val: Value = serde_json::from_str(&data).unwrap();
                        assert_eq!(val, msg);
                        break;
                    }
                    Event::PersistentData(data) => log::info!("persistent {data}"),
                    Event::Config(cfg) => log::info!("config {cfg}"),
                    Event::Discovered(peers) => {
                        log::info!("found peers: {peers:?}");
                    }
                    Event::Ready(peers) => {
                        log::info!("peers ready: {peers:?}");
                    }
                }
            }
        }

        drop(b_s);
        drop(c_s);
    }
}
