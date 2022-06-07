use crate::domopersistentstorage::{DomoPersistentStorage, SqliteStorage};
use crate::utils;
use chrono::prelude::*;
use futures::prelude::*;
use libp2p::gossipsub::IdentTopic as Topic;
use libp2p::swarm::SwarmEvent;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::time::Duration;

// possible events returned by cache_loop_event()
pub enum DomoEvent {
    None,
    VolatileData(serde_json::Value),
    PersistentData(DomoCacheElement),
}

// period at which we send messages containing our cache hash
const SEND_CACHE_HASH_PERIOD: u8 = 5;

pub trait DomoCacheOperations {
    // method to write a DomoCacheElement
    fn insert_cache_element(
        &mut self,
        cache_element: DomoCacheElement,
        persist: bool,
        publish: bool,
    );

    // method to read a DomoCacheElement
    fn read_cache_element(&self, topic_name: &str, topic_uuid: &str) -> Option<DomoCacheElement>;

    // method to write a value in cache, with no timestamp check
    // the publication_timestamp is generated by the method itself
    fn write_value(&mut self, topic_name: &str, topic_uuid: &str, value: Value);

    // method to publish volatile message
    fn pub_value(&mut self, value: Value);

    // method to delete a topic_name, topic_uuid
    fn delete_value(&mut self, topic_name: &str, topic_uuid: &str);

    // method to write a value checking timestamp
    // returns the value in cache if it is more recent that the one used while calling the method
    fn write_with_timestamp_check(
        &mut self,
        topic_name: &str,
        topic_uuid: &str,
        elem: DomoCacheElement,
    ) -> Option<DomoCacheElement>;
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct DomoCacheElement {
    pub topic_name: String,
    pub topic_uuid: String,
    pub value: Value,
    pub deleted: bool,
    pub publication_timestamp: u128,
    pub publisher_peer_id: String,
    pub republication_timestamp: u128,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub struct DomoCacheStateMessage {
    pub peer_id: String,
    pub cache_hash: u64,
    pub publication_timestamp: u128,
}

impl Display for DomoCacheElement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(topic_name: {}, topic_uuid:{}, \
            value: {}, deleted: {}, publication_timestamp: {}, \
            peer_id: {})",
            self.topic_name,
            self.topic_uuid,
            self.value.to_string(),
            self.deleted,
            self.publication_timestamp,
            self.publisher_peer_id
        )
    }
}

pub struct DomoCache<T: DomoPersistentStorage> {
    pub storage: T,
    pub cache: BTreeMap<String, BTreeMap<String, DomoCacheElement>>,
    pub peers_caches_state: BTreeMap<String, DomoCacheStateMessage>,
    pub publish_cache_counter: u8,
    pub last_cache_repub_timestamp: u128,
    pub swarm: libp2p::Swarm<crate::domolibp2p::DomoBehaviour>,
    pub is_persistent_cache: bool,
    pub local_peer_id: String,
}

impl<T: DomoPersistentStorage> Hash for DomoCache<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for (topic_name, map_topic_name) in self.cache.iter() {
            topic_name.hash(state);

            for (topic_uuid, value) in map_topic_name.iter() {
                topic_uuid.hash(state);
                value.to_string().hash(state);
            }
        }
    }
}

impl<T: DomoPersistentStorage> DomoCache<T> {
    fn handle_volatile_data(
        &self,
        message: &str,
    ) -> std::result::Result<DomoEvent, Box<dyn Error>> {
        let m: serde_json::Value = serde_json::from_str(message)?;
        Ok(DomoEvent::VolatileData(m))
    }

    fn handle_persistent_message_data(
        &mut self,
        message: &str,
    ) -> std::result::Result<DomoEvent, Box<dyn Error>> {
        let mut m: DomoCacheElement = serde_json::from_str(message)?;

        // rimetto a 0 il republication timestamp altrimenti cambia hash
        m.republication_timestamp = 0;

        let topic_name = m.topic_name.clone();
        let topic_uuid = m.topic_uuid.clone();

        match self.write_with_timestamp_check(&topic_name, &topic_uuid, m.clone()) {
            None => {
                log::info!("New message received");
                // since a new message arrived, we invalidate peers cache states
                self.peers_caches_state.clear();
                return Ok(DomoEvent::PersistentData(m));
            }
            _ => {
                log::info!("Old message received");
                return Ok(DomoEvent::None);
            }
        }
    }

    // restituisce una tupla (is_synchronized, is_hash_leader)

    fn is_synchronized(
        &self,
        local_hash: u64,
        peers_caches_state: &BTreeMap<String, DomoCacheStateMessage>,
    ) -> (bool, bool) {
        let fil: Vec<u64> = peers_caches_state
            .iter()
            .filter(|(_, data)| {
                (data.cache_hash != local_hash)
                    && (data.publication_timestamp
                        > (utils::get_epoch_ms() - (1000 * 2 * u128::from(SEND_CACHE_HASH_PERIOD))))
            })
            .map(|(_, data)| data.cache_hash)
            .collect();

        // se ci sono hashes diversi dal mio non è consistente
        // verifico se sono il leader per l'hash
        if fil.len() > 0 {
            let fil2: Vec<String> = peers_caches_state
                .iter()
                .filter(|(peer_id, data)| {
                    (data.cache_hash == local_hash)
                        && (self.local_peer_id.cmp(peer_id) == Ordering::Less)
                        && (data.publication_timestamp
                            > (utils::get_epoch_ms()
                                - (1000 * 2 * u128::from(SEND_CACHE_HASH_PERIOD))))
                })
                .map(|(peer_id, _data)| String::from(peer_id))
                .collect();

            if fil2.len() > 0 {
                // non sono il leader dello hash
                return (false, false);
            } else {
                // sono il leader dello hash
                return (false, true);
            }
        }

        // è sincronizzata
        (true, true)
    }

    pub fn get_topic_uuid(&self, topic_name: &str, topic_uuid: &str) -> Result<Value, String> {
        let ret = self.read_cache_element(topic_name, topic_uuid);
        match ret {
            None => {
                return Err(String::from("TopicName TopicUUID not found"));
            }
            Some(cache_element) => {
                return Ok(serde_json::json!({
                            "topic_name": topic_name.clone(),
                            "topic_uuid": topic_uuid.clone(),
                            "value": cache_element.value.clone()
                }));
            }
        }
    }

    pub fn get_topic_name(&self, topic_name: &str) -> Result<Value, String> {
        let s = r#"[]"#;
        let mut ret: Value = serde_json::from_str(s).unwrap();

        match self.cache.get(topic_name) {
            None => {
                return Err(String::from("topic_name not found"));
            }
            Some(topic_name_map) => {
                for (topic_uuid, cache_element) in topic_name_map.iter() {
                    if cache_element.deleted == false {
                        let val = serde_json::json!({
                            "topic_name": topic_name.clone(),
                            "topic_uuid": topic_uuid.clone(),
                            "value": cache_element.value.clone()
                        });
                        ret.as_array_mut().unwrap().push(val);
                    }
                }
                return Ok(ret);
            }
        }
    }

    pub fn get_all(&self) -> Value {
        let s = r#"[]"#;
        let mut ret: Value = serde_json::from_str(s).unwrap();

        for (topic_name, topic_name_map) in self.cache.iter() {
            for (topic_uuid, cache_element) in topic_name_map.iter() {
                if cache_element.deleted == false {
                    let val = serde_json::json!({
                    "topic_name": topic_name.clone(),
                    "topic_uuid": topic_uuid.clone(),
                    "value": cache_element.value.clone()
                            }
                    );
                    ret.as_array_mut().unwrap().push(val);
                }
            }
        }
        ret
    }

    fn publish_cache(&mut self) {
        let mut cache_elements = vec![];

        for (_, topic_name_map) in self.cache.iter() {
            for (_, cache_element) in topic_name_map.iter() {
                cache_elements.push(cache_element.clone());
            }
        }

        for elem in cache_elements {
            self.gossip_pub(elem, true);
        }

        self.last_cache_repub_timestamp = utils::get_epoch_ms();
    }

    fn handle_config_data(&mut self, message: &str) {
        log::info!("Received cache message, check caches ...");
        let m: DomoCacheStateMessage = serde_json::from_str(message).unwrap();
        self.peers_caches_state.insert(m.peer_id.clone(), m);
        self.check_caches_desynchronization();
    }

    fn check_caches_desynchronization(&mut self) {
        let local_hash = self.get_cache_hash();
        let (sync, leader) = self.is_synchronized(local_hash, &self.peers_caches_state);
        if !sync {
            log::info!("Caches are not synchronized");
            if leader {
                log::info!("Publishing my cache since I am the leader for the hash");
                if self.last_cache_repub_timestamp
                    < (utils::get_epoch_ms() - 1000 * u128::from(SEND_CACHE_HASH_PERIOD))
                {
                    self.publish_cache();
                } else {
                    log::info!("Skipping cache repub since it occurred not so much time ago");
                }
            } else {
                log::info!("I am not the leader for the hash");
            }
        } else {
            log::info!("Caches are synchronized");
        }
    }

    fn send_cache_state(&mut self) {
        let m = DomoCacheStateMessage {
            peer_id: self.local_peer_id.to_string(),
            cache_hash: self.get_cache_hash(),
            publication_timestamp: crate::utils::get_epoch_ms(),
        };

        let topic = Topic::new("domo-config");

        let m = serde_json::to_string(&m).unwrap();

        if let Err(e) = self
            .swarm
            .behaviour_mut()
            .gossipsub
            .publish(topic.clone(), m.as_bytes())
        {
            log::info!("Publish error: {:?}", e);
        } else {
            log::info!("Published cache hash");
        }

        self.publish_cache_counter -= 1;
        if self.publish_cache_counter == 0 {
            self.publish_cache_counter = 4;
            self.check_caches_desynchronization();
        }
    }

    pub fn print_peers_cache(&self) {
        for (peer_id, peer_data) in self.peers_caches_state.iter() {
            println!(
                "Peer {}, HASH: {}, TIMESTAMP: {}",
                peer_id, peer_data.cache_hash, peer_data.publication_timestamp
            );
        }
    }

    pub async fn cache_event_loop(&mut self) -> std::result::Result<DomoEvent, Box<dyn Error>> {
        loop {
            tokio::select!(
                // sending cache state periodically
                _ = tokio::time::sleep(Duration::from_secs(u64::from(SEND_CACHE_HASH_PERIOD))) => {
                            self.send_cache_state();
                },

                event = self.swarm.select_next_some() => {
                match event {

                    SwarmEvent::ExpiredListenAddr { address, .. } => {
                        log::info!("Address {:?} expired", address);
                    }
                    SwarmEvent::ConnectionEstablished {..} => {
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
                        println!("Listening in {:?}", address);
                    }
                    SwarmEvent::Behaviour(crate::domolibp2p::OutEvent::Gossipsub(
                        libp2p::gossipsub::GossipsubEvent::Message {
                            propagation_source: _peer_id,
                            message_id: _id,
                            message,
                        },
                    )) => {

                        if message.topic.to_string() == "domo-persistent-data" {
                            return self.handle_persistent_message_data(&String::from_utf8_lossy(&message.data));
                        } else if message.topic.to_string() == "domo-config" {
                            self.handle_config_data(&String::from_utf8_lossy(&message.data));
                        } else if message.topic.to_string() == "domo-volatile-data" {
                            return self.handle_volatile_data(&String::from_utf8_lossy(&message.data));
                        }
                            else {
                                log::info!("Not able to recognize message");
                            }
                    }
                    SwarmEvent::Behaviour(crate::domolibp2p::OutEvent::Mdns(
                        libp2p::mdns::MdnsEvent::Expired(list),
                    )) => {
                        let local = Utc::now();

                        for (peer, _) in list {
                            log::info!("MDNS for peer {} expired {:?}", peer, local);
                        }
                    }
                    SwarmEvent::Behaviour(crate::domolibp2p::OutEvent::Mdns(
                        libp2p::mdns::MdnsEvent::Discovered(list),
                    )) => {
                        let local = Utc::now();
                        for (peer, _) in list {
                            self.swarm
                                .behaviour_mut()
                                .gossipsub
                                .add_explicit_peer(&peer);
                            log::info!("Discovered peer {} {:?}", peer, local);
                        }

                    }
                    _ => {}
                    }
                }
            );
        }
    }

    pub async fn new(is_persistent_cache: bool, storage: T) -> Self {
        let swarm = crate::domolibp2p::start().await.unwrap();
        let peer_id = swarm.local_peer_id().to_string();

        let mut c = DomoCache {
            is_persistent_cache,
            swarm,
            local_peer_id: peer_id,
            publish_cache_counter: 4,
            last_cache_repub_timestamp: 0,
            storage: storage,
            cache: BTreeMap::new(),
            peers_caches_state: BTreeMap::new(),
        };

        // popolo la mia cache con il contenuto dello sqlite

        let ret = c.storage.get_all_elements();

        for elem in ret {
            // non ripubblicp
            c.insert_cache_element(elem, false, false);
        }
        c
    }

    pub fn gossip_pub(&mut self, mut m: DomoCacheElement, republished: bool) {
        let topic = Topic::new("domo-persistent-data");

        if republished {
            m.republication_timestamp = utils::get_epoch_ms();
        } else {
            m.republication_timestamp = 0;
        }

        let m = serde_json::to_string(&m).unwrap();

        if let Err(e) = self
            .swarm
            .behaviour_mut()
            .gossipsub
            .publish(topic.clone(), m.as_bytes())
        {
            log::info!("Publish error: {:?}", e);
        }
    }

    pub fn print(&self) {
        for (topic_name, topic_name_map) in self.cache.iter() {
            let mut first = true;

            for (_, value) in topic_name_map.iter() {
                if value.deleted == false {
                    if first == true {
                        println!("TopicName {} ", topic_name);
                        first = false;
                    }
                    println!("{}", value);
                }
            }
        }
    }

    pub fn print_cache_hash(&self) {
        println!("Hash {}", self.get_cache_hash())
    }

    pub fn get_cache_hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.hash(&mut s);
        s.finish()
    }
}

impl<T: DomoPersistentStorage> DomoCacheOperations for DomoCache<T> {
    fn insert_cache_element(
        &mut self,
        cache_element: DomoCacheElement,
        persist: bool,
        publish: bool,
    ) {
        {
            // topic_name already present
            if self.cache.contains_key(&cache_element.topic_name) {
                self.cache
                    .get_mut(&cache_element.topic_name)
                    .unwrap()
                    .insert(cache_element.topic_uuid.clone(), cache_element.clone());
            } else {
                // first time that we add an element of topic_name type
                self.cache
                    .insert(cache_element.topic_name.clone(), BTreeMap::new());
                self.cache
                    .get_mut(&cache_element.topic_name)
                    .unwrap()
                    .insert(cache_element.topic_uuid.clone(), cache_element.clone());
            }

            if persist {
                self.storage.store(&cache_element);
            }
        }

        if publish {
            self.gossip_pub(cache_element, false);
        }
    }

    fn read_cache_element(&self, topic_name: &str, topic_uuid: &str) -> Option<DomoCacheElement> {
        let value = self.cache.get(topic_name);

        match value {
            Some(topic_map) => match topic_map.get(topic_uuid) {
                Some(element) => {
                    if element.deleted == false {
                        return Some((*element).clone());
                    } else {
                        return None;
                    }
                }
                None => None,
            },
            None => None,
        }
    }

    fn delete_value(&mut self, topic_name: &str, topic_uuid: &str) {
        let timest = utils::get_epoch_ms();
        let elem = DomoCacheElement {
            topic_name: String::from(topic_name),
            topic_uuid: String::from(topic_uuid),
            publication_timestamp: timest,
            value: serde_json::Value::Null,
            deleted: true,
            publisher_peer_id: self.local_peer_id.clone(),
            republication_timestamp: 0,
        };

        self.insert_cache_element(elem.clone(), self.is_persistent_cache, true);
    }

    fn pub_value(&mut self, value: Value) {
        let topic = Topic::new("domo-volatile-data");

        let m = serde_json::to_string(&value).unwrap();

        if let Err(e) = self
            .swarm
            .behaviour_mut()
            .gossipsub
            .publish(topic.clone(), m.as_bytes())
        {
            log::info!("Publish error: {:?}", e);
        }
    }

    // metodo chiamato dall'applicazione, metto in cache e pubblico
    fn write_value(&mut self, topic_name: &str, topic_uuid: &str, value: Value) {
        let timest = utils::get_epoch_ms();
        let elem = DomoCacheElement {
            topic_name: String::from(topic_name),
            topic_uuid: String::from(topic_uuid),
            publication_timestamp: timest,
            value: value.clone(),
            deleted: false,
            publisher_peer_id: self.local_peer_id.clone(),
            republication_timestamp: 0,
        };

        self.insert_cache_element(elem.clone(), self.is_persistent_cache, true);
    }

    fn write_with_timestamp_check(
        &mut self,
        topic_name: &str,
        topic_uuid: &str,
        elem: DomoCacheElement,
    ) -> Option<DomoCacheElement> {
        let ret = self.cache.get(topic_name);

        match ret {
            None => {
                self.insert_cache_element(elem, self.is_persistent_cache, false);
                None
            }
            Some(topic_map) => match topic_map.get(topic_uuid) {
                None => {
                    self.insert_cache_element(elem, self.is_persistent_cache, false);
                    None
                }
                Some(value) => {
                    if elem.publication_timestamp > value.publication_timestamp {
                        self.insert_cache_element(elem, self.is_persistent_cache, false);
                        None
                    } else {
                        Some((*value).clone())
                    }
                }
            },
        }
    }
}

mod tests {
    use super::DomoCacheOperations;
    use crate::domocache::DomoCacheElement;

    #[cfg(test)]
    #[tokio::test]
    async fn test_delete() {
        let storage = super::SqliteStorage::new("./prova.sqlite", true);

        let mut domo_cache = super::DomoCache::new(true, storage).await;

        domo_cache.write_value(
            "Domo::Light",
            "luce-delete",
            serde_json::json!({ "connected": true}),
        );

        let v = domo_cache
            .read_cache_element("Domo::Light", "luce-delete")
            .unwrap();

        domo_cache.delete_value("Domo::Light", "luce-delete");

        let v = domo_cache.read_cache_element("Domo::Light", "luce-delete");

        assert_eq!(v, None);
    }

    #[cfg(test)]
    #[tokio::test]
    async fn test_write_and_read_key() {
        let storage = super::SqliteStorage::new("./prova.sqlite", true);
        let mut domo_cache = super::DomoCache::new(true, storage).await;

        domo_cache.write_value(
            "Domo::Light",
            "luce-1",
            serde_json::json!({ "connected": true}),
        );

        let val = domo_cache
            .read_cache_element("Domo::Light", "luce-1")
            .unwrap()
            .value;
        assert_eq!(serde_json::json!({ "connected": true}), val)
    }

    #[tokio::test]
    async fn test_write_twice_same_key() {
        let storage = super::SqliteStorage::new("./prova.sqlite", true);

        let mut domo_cache = super::DomoCache::new(true, storage).await;

        domo_cache.write_value(
            "Domo::Light",
            "luce-1",
            serde_json::json!({ "connected": true}),
        );

        let val = domo_cache
            .read_cache_element("Domo::Light", "luce-1")
            .unwrap()
            .value;

        assert_eq!(serde_json::json!({ "connected": true}), val);

        domo_cache.write_value(
            "Domo::Light",
            "luce-1",
            serde_json::json!({ "connected": false}),
        );

        let val = domo_cache
            .read_cache_element("Domo::Light", "luce-1")
            .unwrap()
            .value;

        assert_eq!(serde_json::json!({ "connected": false}), val)
    }

    #[tokio::test]
    async fn test_write_old_timestamp() {
        let storage = super::SqliteStorage::new("./prova.sqlite", true);

        let mut domo_cache = super::DomoCache::new(true, storage).await;

        domo_cache.write_value(
            "Domo::Light",
            "luce-timestamp",
            serde_json::json!({ "connected": true}),
        );

        let old_val = domo_cache
            .read_cache_element("Domo::Light", "luce-timestamp")
            .unwrap();

        let el = super::DomoCacheElement {
            topic_name: String::from("Domo::Light"),
            topic_uuid: String::from("luce-timestamp"),
            value: Default::default(),
            deleted: false,
            publication_timestamp: 0,
            publisher_peer_id: domo_cache.local_peer_id.clone(),
            republication_timestamp: 0,
        };

        // mi aspetto il vecchio valore perchè non deve fare la scrittura

        let ret = domo_cache
            .write_with_timestamp_check("Domo::Light", "luce-timestamp", el)
            .unwrap();

        // mi aspetto di ricevere il vecchio valore
        assert_eq!(ret, old_val);

        domo_cache.write_value(
            "Domo::Light",
            "luce-timestamp",
            serde_json::json!({ "connected": false}),
        );

        let val = domo_cache
            .read_cache_element("Domo::Light", "luce-timestamp")
            .unwrap();

        assert_ne!(ret, val);
    }
}
