use futures::{prelude::*, select};
use libp2p::mdns::{Mdns, MdnsConfig, MdnsEvent};
use libp2p::wasm_ext::ffi::ConnectionEvent;
use libp2p::{gossipsub, swarm::SwarmEvent, Multiaddr};
use rusqlite::{params, Connection, OpenFlags, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::error::Error;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use libp2p::gossipsub::{
    Gossipsub, GossipsubEvent, GossipsubMessage, IdentTopic as Topic, MessageAuthenticity,
    ValidationMode,
};

pub fn get_epoch_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

pub trait DomoPersistentStorage {
    fn store(&mut self, element: &DomoCacheElement);
    fn get_all_elements(&mut self) -> Vec<DomoCacheElement>;
}

pub struct SqliteStorage {
    pub house_uuid: String,
    pub sqlite_file: String,
    pub sqlite_connection: Connection,
}

impl SqliteStorage {
    pub fn new(house_uuid: &str, sqlite_file: &str, write_access: bool) -> Self {
        let conn = if write_access == false {
            match Connection::open_with_flags(sqlite_file, OpenFlags::SQLITE_OPEN_READ_ONLY) {
                Ok(conn) => conn,
                _ => {
                    panic!("Error while opening the sqlite DB");
                }
            }
        } else {
            let conn = match Connection::open(sqlite_file) {
                Ok(conn) => conn,
                _ => {
                    panic!("Error while opening the sqlite DB");
                }
            };

            let res = conn
                .execute(
                    "CREATE TABLE IF NOT EXISTS domo_data (
                  topic_name             TEXT,
                  topic_uuid             TEXT,
                  value                  TEXT,
                  deleted                INTEGER,
                  publication_timestamp   TEXT,
                  PRIMARY KEY (topic_name, topic_uuid)
                  )",
                    [],
                )
                .unwrap();

            conn
        };

        SqliteStorage {
            house_uuid: house_uuid.to_owned(),
            sqlite_file: sqlite_file.to_owned(),
            sqlite_connection: conn,
        }
    }
}

impl DomoPersistentStorage for SqliteStorage {
    fn store(&mut self, element: &DomoCacheElement) {
        let ret = match self.sqlite_connection.execute(
            "INSERT OR REPLACE INTO domo_data\
             (topic_name, topic_uuid, value, deleted, publication_timestamp)\
              VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                element.topic_name,
                element.topic_uuid,
                element.value.to_string(),
                element.deleted,
                element.publication_timestamp.to_string()
            ],
        ) {
            Ok(ret) => ret,
            _ => {
                panic!("Error while executing write operation on sqlite")
            }
        };
    }

    fn get_all_elements(&mut self) -> Vec<DomoCacheElement> {
        // read all not deleted elements
        let mut stmt = self
            .sqlite_connection
            .prepare("SELECT * FROM domo_data WHERE deleted=0")
            .unwrap();

        let values_iter = stmt
            .query_map([], |row| {
                let jvalue: String = row.get(2)?;
                let jvalue = serde_json::from_str(&jvalue);

                let pub_timestamp_string: String = row.get(4)?;

                Ok(DomoCacheElement {
                    topic_name: row.get(0)?,
                    topic_uuid: row.get(1)?,
                    value: jvalue.unwrap(),
                    deleted: row.get(3)?,
                    publication_timestamp: pub_timestamp_string.parse().unwrap(),
                })
            })
            .unwrap();

        let mut ret = vec![];
        for val in values_iter {
            let v = val.unwrap();
            ret.push(v);
        }

        ret
    }
}

pub trait DomoCacheOperations {
    // method to write a DomoCacheElement
    fn insert_cache_element(
        &mut self,
        cache_element: DomoCacheElement,
        persist: bool,
        publish: bool,
    );

    // method to read a DomoCacheElement
    fn read_cache_element(
        &mut self,
        topic_name: &str,
        topic_uuid: &str,
    ) -> Option<DomoCacheElement>;

    // method to write a value in cache, with no timestamp check
    // the publication_timestamp is generated by the method itself
    fn write_value(&mut self, topic_name: &str, topic_uuid: &str, value: Value);

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
}

pub struct DomoCache<T: DomoPersistentStorage> {
    pub house_uuid: String,
    pub is_persistent_cache: bool,
    pub storage: T,
    pub cache: HashMap<String, HashMap<String, DomoCacheElement>>,
    pub swarm: libp2p::Swarm<crate::domolibp2p::DomoBehaviour>,
}

impl<T: DomoPersistentStorage> DomoCache<T> {
    pub async fn wait_for_messages(
        &mut self,
    ) -> std::result::Result<DomoCacheElement, Box<dyn Error>> {
        loop {
            let event = self.swarm.select_next_some().await;
            match event {
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("Listening in {:?}", address);
                }
                SwarmEvent::Behaviour(crate::domolibp2p::OutEvent::Gossipsub(
                    GossipsubEvent::Message {
                        propagation_source: peer_id,
                        message_id: id,
                        message,
                    },
                )) => {
                    println!(
                        "Got message: {} with id: {} from peer: {:?}, topic {}",
                        String::from_utf8_lossy(&message.data),
                        id,
                        peer_id,
                        &message.topic
                    );

                    let m: DomoCacheElement =
                        serde_json::from_str(&String::from_utf8_lossy(&message.data))?;

                    let topic_name = m.topic_name.clone();
                    let topic_uuid = m.topic_uuid.clone();

                    match self.write_with_timestamp_check(&topic_name, &topic_uuid, m.clone()) {
                        None => {
                            println!("New message received");
                            return Ok(m);
                        }
                        _ => {
                            println!("Old message received");
                        }
                    }
                }
                SwarmEvent::Behaviour(crate::domolibp2p::OutEvent::Mdns(
                    libp2p::mdns::MdnsEvent::Discovered(list),
                )) => {
                    for (peer, _) in list {
                        self.swarm
                            .behaviour_mut()
                            .gossipsub
                            .add_explicit_peer(&peer);
                        println!("Discovered peer {}", peer);
                    }
                }
                _ => {}
            }
        }
    }

    pub async fn new(house_uuid: &str, is_persistent_cache: bool, storage: T) -> Self {
        let mut swarm = crate::domolibp2p::start().await.unwrap();

        let mut c = DomoCache {
            house_uuid: house_uuid.to_owned(),
            is_persistent_cache: is_persistent_cache,
            storage: storage,
            cache: HashMap::new(),
            swarm: swarm,
        };

        // popolo la mia cache con il contenuto dello sqlite
        let ret = c.storage.get_all_elements();

        for elem in ret {
            // non ripubblicp
            c.insert_cache_element(elem, false, false);
        }
        c
    }

    pub fn gossip_pub(&mut self, topic_name: &str, topic_uuid: &str, m: DomoCacheElement) {
        let topic = Topic::new("domo-data");

        let m = serde_json::to_string(&m).unwrap();

        if let Err(e) = self
            .swarm
            .behaviour_mut()
            .gossipsub
            .publish(topic.clone(), m.as_bytes())
        {
            println!("Publish error: {:?}", e);
        } else {
            println!("Publishing message");
        }
    }

    pub fn print(&self) {
        for (topic_name, topic_name_map) in self.cache.iter() {
            println!(" TopicName {} ", topic_name);

            for (topic_uuid, value) in topic_name_map.iter() {
                println!("uuid: {} value: {}", topic_uuid, value.value.to_string());
            }
        }
    }
}

impl<T: DomoPersistentStorage> DomoCacheOperations for DomoCache<T> {
    fn insert_cache_element(
        &mut self,
        cache_element: DomoCacheElement,
        persist: bool,
        publish: bool,
    ) {
        // topic_name already present
        if self.cache.contains_key(&cache_element.topic_name) {
            self.cache
                .get_mut(&cache_element.topic_name)
                .unwrap()
                .insert(cache_element.topic_uuid.clone(), cache_element.clone());
        } else {
            // first time that we add an element of topic_name type
            self.cache
                .insert(cache_element.topic_name.clone(), HashMap::new());
            self.cache
                .get_mut(&cache_element.topic_name)
                .unwrap()
                .insert(cache_element.topic_uuid.clone(), cache_element.clone());
        }

        if persist {
            self.storage.store(&cache_element);
        }

        if publish {
            let topic_name = cache_element.topic_name.clone();
            let topic_uuid = cache_element.topic_uuid.clone();

            self.gossip_pub(&topic_name, &topic_uuid, cache_element);
        }
    }

    fn read_cache_element(
        &mut self,
        topic_name: &str,
        topic_uuid: &str,
    ) -> Option<DomoCacheElement> {
        let value = self.cache.get(topic_name);

        match value {
            Some(topic_map) => match topic_map.get(topic_uuid) {
                Some(element) => Some((*element).clone()),
                None => None,
            },
            None => None,
        }
    }

    // metodo chiamato dall'applicazione, metto in cache e pubblico
    fn write_value(&mut self, topic_name: &str, topic_uuid: &str, value: Value) {
        let timest = get_epoch_ms();
        let elem = DomoCacheElement {
            topic_name: String::from(topic_name),
            topic_uuid: String::from(topic_uuid),
            publication_timestamp: timest,
            value: value.clone(),
            deleted: false,
        };

        self.insert_cache_element(elem.clone(), self.is_persistent_cache, true);
    }

    fn write_with_timestamp_check(
        &mut self,
        topic_name: &str,
        topic_uuid: &str,
        elem: DomoCacheElement,
    ) -> Option<DomoCacheElement> {
        match self.read_cache_element(topic_name, topic_uuid) {
            Some(value) => {
                if value.publication_timestamp > elem.publication_timestamp {
                    Some(value)
                } else {
                    // inserisco in cache ma non ripubblico
                    self.insert_cache_element(elem, self.is_persistent_cache, false);
                    None
                }
            }
            None => {
                // inserisco in cache ma non ripubblico
                self.insert_cache_element(elem, self.is_persistent_cache, false);
                None
            }
        }
    }
}

mod tests {
    use super::DomoCacheOperations;
    use crate::domolibp2p::DomoBehaviour;

    #[cfg(test)]
    #[test]
    fn test_write_and_read_key() {
        let house_uuid = "CasaProva";
        let storage = super::SqliteStorage::new(house_uuid, "./prova.sqlite", true);
        let mut domo_cache = super::DomoCache::new(house_uuid, true, storage);

        domo_cache.print();

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

    #[test]
    fn test_write_twice_same_key() {
        let house_uuid = "CasaProva";
        let storage = super::SqliteStorage::new(house_uuid, "./prova.sqlite", true);

        let mut domo_cache = super::DomoCache::new(house_uuid, true, storage);

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

    #[test]
    fn test_write_old_timestamp() {
        let house_uuid = "CasaProva";
        let storage = super::SqliteStorage::new(house_uuid, "./prova.sqlite", true);

        let mut domo_cache = super::DomoCache::new(house_uuid, true, storage);

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
