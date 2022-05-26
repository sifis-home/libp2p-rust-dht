mod domocache;
mod domolibp2p;

use async_std::{io, task};
use futures::{prelude::*, select};
use std::collections::HashMap;

// Gossip includes
use libp2p::gossipsub::MessageId;
use libp2p::gossipsub::{
    GossipsubEvent, GossipsubMessage, IdentTopic as Topic, MessageAuthenticity, ValidationMode,
};
use libp2p::{gossipsub, swarm::SwarmEvent, Multiaddr};

use std::collections::hash_map::DefaultHasher;
use std::env;
use std::hash::{Hash, Hasher};

//

use crate::domocache::DomoCacheOperations;
use libp2p::{
    development_transport, identity,
    mdns::{Mdns, MdnsConfig, MdnsEvent},
    swarm::NetworkBehaviourEventProcess,
    NetworkBehaviour, PeerId, Swarm,
};
use serde_json::{json, Value};
use std::error::Error;
use std::time::Duration;

#[async_std::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Usage: ./domo-libp2p <sqlite_file_path>");

    let args: Vec<String> = env::args().collect();

    let sqlite_file = &args[1];

    env_logger::init();

    // resto in attesa della creazione dello swarm

    let mut swarm = domolibp2p::start().await.unwrap();

    let house_uuid = "CasaProva";

    let storage = domocache::SqliteStorage::new(house_uuid, sqlite_file, true);

    let mut domo_cache = domocache::DomoCache::new(house_uuid, true, storage);

    let mut stdin = io::BufReader::new(io::stdin()).lines().fuse();

    // idle loop
    loop {
        select! {
            line = stdin.select_next_some() => {
                let line = line.expect("Stdin error");
                let mut args = line.split(" ");

                match args.next(){
                    Some("PUB") => {
                        let topic_name = args.next();

                        let topic_uuid = args.next();

                        let value = args.next();

                        // se uno degli argomenti è vuoto
                        if topic_name == None || topic_uuid == None || value == None{
                            println!("topic_name, topic_uuid, value are mandatory arguments");
                        } else{
                            let topic_name= topic_name.unwrap();
                            let topic_uuid= topic_uuid.unwrap();
                            let value = value.unwrap();

                            println!("{} {} {}", topic_name, topic_uuid, value);

                            let val = json!({ "payload": value,
                                "topic_name": topic_name, "topic_uuid": topic_uuid});

                            let m = domocache::DomoMessage {
                                topic_name: topic_name.to_owned(),
                                topic_uuid: topic_uuid.to_owned(),
                                payload: val.clone()
                            };


                            domo_cache.write_value(topic_name, topic_uuid, val);
                            domolibp2p::pub_element(&mut swarm, topic_name, topic_uuid, m);
                        }
                    },
                    _ => {
                        println!("expected PUB <topic_name> <topic_uuid> <value>");
                    }
                }

            }
            event = swarm.select_next_some() => match event {
                SwarmEvent::NewListenAddr { address, .. } => {
                    println!("Listening in {:?}", address);
                },
                SwarmEvent::Behaviour(
                    domolibp2p::OutEvent::Gossipsub(
                    GossipsubEvent::Message{
                    propagation_source: peer_id,
                    message_id: id,
                    message
                        })) => {
                    println!(
                        "Got message: {} with id: {} from peer: {:?}, topic {}",
                        String::from_utf8_lossy(&message.data),
                        id,
                        peer_id,
                        &message.topic);

                    let m : domocache::DomoMessage = serde_json::from_str(&String::from_utf8_lossy(&message.data))?;

                    domo_cache.write_value(&m.topic_name, &m.topic_uuid, m.payload);
                },
                SwarmEvent::Behaviour(domolibp2p::OutEvent::Mdns(
                    MdnsEvent::Discovered(list)
                )) => {
                    for (peer, _) in list {
                        swarm
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
}
