// Gossip includes
use libp2p::gossipsub::MessageId;
use libp2p::gossipsub::{
    // Gossipsub, GossipsubEvent, GossipsubMessage,
    IdentTopic as Topic,
    MessageAuthenticity,
    ValidationMode,
};
use libp2p::{gossipsub, tcp};

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
//

use libp2p::core::{muxing::StreamMuxerBox, transport, transport::upgrade::Version};

use libp2p::noise;
use libp2p::pnet::{PnetConfig, PreSharedKey};
use libp2p::yamux;
//use libp2p::tcp::TcpConfig;
use libp2p::Transport;

use libp2p::{identity, mdns, swarm::NetworkBehaviour, PeerId, Swarm};

use std::error::Error;
use std::io;
use std::time::Duration;

const KEY_SIZE: usize = 32;

fn parse_hex_key(s: &str) -> Result<[u8; KEY_SIZE], String> {
    if s.len() == KEY_SIZE * 2 {
        let mut r = [0u8; KEY_SIZE];
        for i in 0..KEY_SIZE {
            let ret = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16);
            match ret {
                Ok(res) => {
                    r[i] = res;
                }
                Err(_e) => return Err(String::from("Error while parsing")),
            }
        }
        Ok(r)
    } else {
        Err(format!(
            "Len Error: expected {} but got {}",
            KEY_SIZE * 2,
            s.len()
        ))
    }
}

pub fn build_transport(
    key_pair: identity::Keypair,
    psk: PreSharedKey,
) -> transport::Boxed<(PeerId, StreamMuxerBox)> {
    let noise_config = noise::Config::new(&key_pair).unwrap();
    let yamux_config = yamux::Config::default();

    let base_transport = tcp::tokio::Transport::new(tcp::Config::default().nodelay(true));

    base_transport
        .and_then(move |socket, _| PnetConfig::new(psk).handshake(socket))
        .upgrade(Version::V1)
        .authenticate(noise_config)
        .multiplex(yamux_config)
        .timeout(Duration::from_secs(20))
        .boxed()
}

pub async fn start(
    shared_key: String,
    local_key_pair: identity::Keypair,
    loopback_only: bool,
    listen_addr: String
) -> Result<Swarm<DomoBehaviour>, Box<dyn Error>> {
    let local_peer_id = PeerId::from(local_key_pair.public());



    let arr = parse_hex_key(&shared_key)?;
    let psk = PreSharedKey::new(arr);


    let mut swarm = libp2p::SwarmBuilder::with_existing_identity(local_key_pair)
        .with_tokio()
        .with_other_transport(|key| {
            let noise_config = noise::Config::new(key).unwrap();
            let yamux_config = yamux::Config::default();

            let base_transport = tcp::tokio::Transport::new(tcp::Config::default().nodelay(true));
            let maybe_encrypted = base_transport.and_then(move |socket, _| PnetConfig::new(psk).handshake(socket));
            maybe_encrypted
                .upgrade(Version::V1Lazy)
                .authenticate(noise_config)
                .multiplex(yamux_config)
        })?
        .with_behaviour(|key| {

            let mdnsconf = mdns::Config {
                ttl: Duration::from_secs(10),
                query_interval: Duration::from_secs(5),
                enable_ipv6: false
            };

            let mdns = mdns::tokio::Behaviour::new(mdnsconf, local_peer_id)?;

            // To content-address message, we can take the hash of message and use it as an ID.
            let message_id_fn = |message: &gossipsub::Message| {
                let mut s = DefaultHasher::new();
                message.data.hash(&mut s);
                MessageId::from(s.finish().to_string())
            };

            let gossipsub_config = gossipsub::ConfigBuilder::default()
                .heartbeat_interval(Duration::from_secs(1))
                .check_explicit_peers_ticks(5)// This is set to aid debugging by not cluttering the log space
                .validation_mode(gossipsub::ValidationMode::Strict) // This sets the kind of message validation. The default is Strict (enforce message signing)
                .message_id_fn(message_id_fn) // content-address messages. No two messages of the same content will be propagated.
                .build()
                .map_err(|msg| io::Error::new(io::ErrorKind::Other, msg))?; // Temporary hack because `build` does not return a proper `std::error::Error`.

            // build a gossipsub network behaviour
            let gossipsub = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()),
                gossipsub_config,
            )?;

            let behaviour = DomoBehaviour { mdns, gossipsub };

            Ok(behaviour)

        })?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(1)))
        .build();


    // Create a Gossipsub topic
    let topic_persistent_data = gossipsub::IdentTopic::new("domo-persistent-data");
    let topic_volatile_data = gossipsub::IdentTopic::new("domo-volatile-data");
    let topic_config = gossipsub::IdentTopic::new("domo-config");

    swarm.behaviour_mut().gossipsub.subscribe(&topic_persistent_data)?;
    swarm.behaviour_mut().gossipsub.subscribe(&topic_volatile_data)?;
    swarm.behaviour_mut().gossipsub.subscribe(&topic_config)?;

    // Listen on all interfaces and whatever port the OS assigns.
    if !loopback_only {
        swarm.listen_on(listen_addr.parse()?)?;
    } else {
        swarm.listen_on("/ip4/127.0.0.1/tcp/0".parse()?)?;

    }

    Ok(swarm)
}

// We create a custom network behaviour that combines mDNS and gossipsub.
#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "OutEvent")]
pub struct DomoBehaviour {
    pub mdns: mdns::tokio::Behaviour,
    pub gossipsub: gossipsub::Behaviour,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum OutEvent {
    Gossipsub(gossipsub::Event),
    Mdns(mdns::Event),
}

impl From<mdns::Event> for OutEvent {
    fn from(v: mdns::Event) -> Self {
        Self::Mdns(v)
    }
}

impl From<gossipsub::Event> for OutEvent {
    fn from(v: gossipsub::Event) -> Self {
        Self::Gossipsub(v)
    }
}
