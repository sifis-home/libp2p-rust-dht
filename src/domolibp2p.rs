// Gossip includes
use libp2p::gossipsub::MessageId;
use libp2p::gossipsub::{
    Gossipsub, GossipsubEvent, GossipsubMessage, IdentTopic as Topic, MessageAuthenticity,
    ValidationMode,
};
use libp2p::swarm::derive_prelude::{ConnectionId, FromSwarm, ListenerId};
use libp2p::swarm::SwarmBuilder;
use libp2p::{gossipsub, tcp};

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
//

use libp2p::core::{
    either::EitherTransport, muxing::StreamMuxerBox, transport, transport::upgrade::Version,
};

use libp2p::noise;
use libp2p::pnet::{PnetConfig, PreSharedKey};
use libp2p::yamux::YamuxConfig;
//use libp2p::tcp::TcpConfig;
use libp2p::Transport;

use libp2p::{identity, mdns, swarm::NetworkBehaviour, PeerId, Swarm};

use std::error::Error;
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
        Err(String::from("Len Error"))
    }
}

pub fn build_transport(
    key_pair: identity::Keypair,
    psk: Option<PreSharedKey>,
) -> transport::Boxed<(PeerId, StreamMuxerBox)> {
    let noise_keys = noise::Keypair::<noise::X25519Spec>::new()
        .into_authentic(&key_pair)
        .unwrap();
    let noise_config = noise::NoiseConfig::xx(noise_keys).into_authenticated();
    let yamux_config = YamuxConfig::default();

    let base_transport = tcp::tokio::Transport::new(tcp::Config::default().nodelay(true));
    let maybe_encrypted = match psk {
        Some(psk) => EitherTransport::Left(
            base_transport.and_then(move |socket, _| PnetConfig::new(psk).handshake(socket)),
        ),
        None => EitherTransport::Right(base_transport),
    };
    maybe_encrypted
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
) -> Result<Swarm<DomoBehaviour>, Box<dyn Error>> {
    let local_peer_id = PeerId::from(local_key_pair.public());

    // Create a Gossipsub topic
    let topic_persistent_data = Topic::new("domo-persistent-data");
    let topic_volatile_data = Topic::new("domo-volatile-data");
    let topic_config = Topic::new("domo-config");

    let arr = parse_hex_key(&shared_key);
    let psk = match arr {
        Ok(s) => Some(PreSharedKey::new(s)),
        Err(_e) => panic!("Invalid key"),
    };

    let transport = build_transport(local_key_pair.clone(), psk);

    // Create a swarm to manage peers and events.
    let mut swarm = {
        let mdnsconf = mdns::Config {
            ttl: Duration::from_secs(600),
            query_interval: Duration::from_secs(580),
            enable_ipv6: false,
        };

        let mdns = mdns::tokio::Behaviour::new(mdnsconf)?;

        // To content-address message, we can take the hash of message and use it as an ID.
        let message_id_fn = |message: &GossipsubMessage| {
            let mut s = DefaultHasher::new();
            message.data.hash(&mut s);
            MessageId::from(s.finish().to_string())
        };

        // Set a custom gossipsub
        let gossipsub_config = gossipsub::GossipsubConfigBuilder::default()
            .idle_timeout(Duration::from_secs(60 * 60 * 24))
            .heartbeat_interval(Duration::from_secs(10)) // This is set to aid debugging by not cluttering the log space
            .validation_mode(ValidationMode::Strict) // This sets the kind of message validation. The default is Strict (enforce message signing)
            .message_id_fn(message_id_fn) // content-address messages. No two messages of the
            // same content will be propagated.
            .build()
            .expect("Valid config");

        // build a gossipsub network behaviour
        let mut gossipsub: gossipsub::Gossipsub = gossipsub::Gossipsub::new(
            MessageAuthenticity::Signed(local_key_pair),
            gossipsub_config,
        )
        .expect("Correct configuration");

        // subscribes to persistent data topic
        gossipsub.subscribe(&topic_persistent_data).unwrap();

        // subscribes to volatile data topic
        gossipsub.subscribe(&topic_volatile_data).unwrap();

        // subscribes to config topic
        gossipsub.subscribe(&topic_config).unwrap();

        let gossipsub = SharedGossipsub(Arc::new(std::sync::Mutex::new(gossipsub)));
        let behaviour = DomoBehaviour { mdns, gossipsub };
        //Swarm::new(transport, behaviour, local_peer_id)

        SwarmBuilder::with_tokio_executor(transport, behaviour, local_peer_id).build()
    };

    if !loopback_only {
        // Listen on all interfaces and whatever port the OS assigns.
        swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
    } else {
        // Listen only on loopack interface
        swarm.listen_on("/ip4/127.0.0.1/tcp/0".parse()?)?;
    }

    Ok(swarm)
}

// We create a custom network behaviour that combines mDNS and gossipsub.
#[derive(NetworkBehaviour)]
#[behaviour(out_event = "OutEvent")]
pub struct DomoBehaviour {
    pub mdns: libp2p::mdns::tokio::Behaviour,
    pub(crate) gossipsub: SharedGossipsub,
}

// See inner doc, this MUST be pub because of `NetworkBehavior` proc-macro. At least we don't
// expose the private doc...
#[derive(Clone)]
pub struct SharedGossipsub(
    /// A workaround for the _opinionated_ libp2p architecture.
    ///
    /// libp2p like to use single abstractions for different purposes, especially in relationship
    /// to [`Swarm`] and [`NetworkBehaviour`]. This approach does not work _at all_ when you try to
    /// correctly handle streams: polling requires a mutable reference to the object, and if you
    /// performs this operation concurrently (and you probably should) then you totally lost the
    /// possibility of accessing the object in other ways. This concept is the reason we have
    /// senders and receivers when using channels.
    ///
    /// Nevertheless, we have been able to build a channel-like interface over
    /// [`Swarm<DomoBehaviour>`], which however uses a _sync_ `Mutex` wrapped inside an `Arc` in
    /// order to access the `gossipsub` member of `DomoBehaviour` after a message has been polled
    /// from the _swarm_ stream. See `domocache::swarm::SwarmReceiver::into_stream` for more
    /// information.
    pub(crate) Arc<std::sync::Mutex<Gossipsub>>,
);

impl SharedGossipsub {
    #[inline]
    fn get_mut(&self) -> std::sync::MutexGuard<'_, Gossipsub> {
        self.0.try_lock().unwrap()
    }
}

impl NetworkBehaviour for SharedGossipsub {
    type ConnectionHandler = <Gossipsub as NetworkBehaviour>::ConnectionHandler;
    type OutEvent = <Gossipsub as NetworkBehaviour>::OutEvent;

    #[inline]
    fn new_handler(&mut self) -> Self::ConnectionHandler {
        self.get_mut().new_handler()
    }

    #[inline]
    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        event: <<Self::ConnectionHandler as libp2p::swarm::IntoConnectionHandler>::Handler as
            libp2p::swarm::ConnectionHandler>::OutEvent,
    ) {
        self.get_mut()
            .on_connection_handler_event(peer_id, connection_id, event)
    }

    #[inline]
    fn poll(
        &mut self,
        cx: &mut std::task::Context<'_>,
        params: &mut impl libp2p::swarm::PollParameters,
    ) -> std::task::Poll<
        libp2p::swarm::NetworkBehaviourAction<Self::OutEvent, Self::ConnectionHandler>,
    > {
        self.get_mut().poll(cx, params)
    }

    #[inline]
    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<libp2p::Multiaddr> {
        self.get_mut().addresses_of_peer(peer_id)
    }

    #[inline]
    fn on_swarm_event(&mut self, event: FromSwarm<Self::ConnectionHandler>) {
        self.get_mut().on_swarm_event(event)
    }

    #[inline]
    fn inject_connection_established(
        &mut self,
        peer_id: &PeerId,
        connection_id: &ConnectionId,
        endpoint: &libp2p::core::ConnectedPoint,
        failed_addresses: Option<&Vec<libp2p::Multiaddr>>,
        other_established: usize,
    ) {
        #[allow(deprecated)]
        self.get_mut().inject_connection_established(
            peer_id,
            connection_id,
            endpoint,
            failed_addresses,
            other_established,
        )
    }

    #[inline]
    fn inject_connection_closed(
        &mut self,
        peer_id: &PeerId,
        connection_id: &ConnectionId,
        endpoint: &libp2p::core::ConnectedPoint,
        handler: <Self::ConnectionHandler as libp2p::swarm::IntoConnectionHandler>::Handler,
        remaining_established: usize,
    ) {
        #[allow(deprecated)]
        self.get_mut().inject_connection_closed(
            peer_id,
            connection_id,
            endpoint,
            handler,
            remaining_established,
        )
    }

    #[inline]
    fn inject_address_change(
        &mut self,
        peer_id: &PeerId,
        connection_id: &ConnectionId,
        old: &libp2p::core::ConnectedPoint,
        new: &libp2p::core::ConnectedPoint,
    ) {
        #[allow(deprecated)]
        self.get_mut()
            .inject_address_change(peer_id, connection_id, old, new)
    }

    #[inline]
    fn inject_event(
        &mut self,
        peer_id: PeerId,
        connection: ConnectionId,
        event: <<Self::ConnectionHandler as libp2p::swarm::IntoConnectionHandler>::Handler as libp2p::swarm::ConnectionHandler>::OutEvent,
    ) {
        #[allow(deprecated)]
        self.get_mut().inject_event(peer_id, connection, event)
    }

    #[inline]
    fn inject_dial_failure(
        &mut self,
        peer_id: Option<PeerId>,
        handler: Self::ConnectionHandler,
        error: &libp2p::swarm::DialError,
    ) {
        #[allow(deprecated)]
        self.get_mut().inject_dial_failure(peer_id, handler, error)
    }

    #[inline]
    fn inject_listen_failure(
        &mut self,
        local_addr: &libp2p::Multiaddr,
        send_back_addr: &libp2p::Multiaddr,
        handler: Self::ConnectionHandler,
    ) {
        #[allow(deprecated)]
        self.get_mut()
            .inject_listen_failure(local_addr, send_back_addr, handler)
    }

    #[inline]
    fn inject_new_listener(&mut self, id: ListenerId) {
        #[allow(deprecated)]
        self.get_mut().inject_new_listener(id)
    }

    #[inline]
    fn inject_new_listen_addr(&mut self, id: ListenerId, addr: &libp2p::Multiaddr) {
        #[allow(deprecated)]
        self.get_mut().inject_new_listen_addr(id, addr)
    }

    #[inline]
    fn inject_expired_listen_addr(&mut self, id: ListenerId, addr: &libp2p::Multiaddr) {
        #[allow(deprecated)]
        self.get_mut().inject_expired_listen_addr(id, addr)
    }

    #[inline]
    fn inject_listener_error(&mut self, id: ListenerId, err: &(dyn std::error::Error + 'static)) {
        #[allow(deprecated)]
        self.get_mut().inject_listener_error(id, err)
    }

    #[inline]
    fn inject_listener_closed(&mut self, id: ListenerId, reason: Result<(), &std::io::Error>) {
        #[allow(deprecated)]
        self.get_mut().inject_listener_closed(id, reason)
    }

    #[inline]
    fn inject_new_external_addr(&mut self, addr: &libp2p::Multiaddr) {
        #[allow(deprecated)]
        self.get_mut().inject_new_external_addr(addr)
    }

    #[inline]
    fn inject_expired_external_addr(&mut self, addr: &libp2p::Multiaddr) {
        #[allow(deprecated)]
        self.get_mut().inject_expired_external_addr(addr)
    }
}

#[derive(Debug)]
pub enum OutEvent {
    Gossipsub(GossipsubEvent),
    Mdns(mdns::Event),
}

impl From<mdns::Event> for OutEvent {
    fn from(v: mdns::Event) -> Self {
        Self::Mdns(v)
    }
}

impl From<GossipsubEvent> for OutEvent {
    fn from(v: GossipsubEvent) -> Self {
        Self::Gossipsub(v)
    }
}
