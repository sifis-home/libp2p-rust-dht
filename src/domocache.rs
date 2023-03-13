mod swarm;

use crate::domocache::swarm::SendError;
use crate::domopersistentstorage::DomoPersistentStorage;
use crate::restmessage::RestResponder;
use crate::websocketmessage::SyncWebSocketDomoResponseMessage;
use crate::{domolibp2p, utils};
use futures::{stream, FutureExt, Stream, StreamExt};
use futures_concurrency::stream::Merge;
use libp2p::core::either::EitherError;
use libp2p::gossipsub::error::GossipsubHandlerError;
use libp2p::gossipsub::IdentTopic as Topic;
use libp2p::identity::Keypair;
use libp2p::swarm::SwarmEvent;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::future::{ready, Ready};
use std::hash::{Hash, Hasher};
use std::ops::{ControlFlow, Not};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{Sender, UnboundedReceiver, UnboundedSender};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::wrappers::{IntervalStream, ReceiverStream, UnboundedReceiverStream};
use void::Void;

use self::swarm::{SwarmReceiver, SwarmSender};

// possible events returned by cache_loop_event()
#[derive(Debug, Clone)]
pub enum DomoEvent {
    None,
    VolatileData(Value),
    PersistentData(DomoCacheElement),
}

// period at which we send messages containing our cache hash
const SEND_CACHE_HASH_PERIOD: u8 = 5;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct DomoCacheElement {
    pub topic_name: String,
    pub topic_uuid: String,
    pub value: Value,
    pub deleted: bool,
    pub publication_timestamp: u128,
    pub publisher_peer_id: String,
    pub republication_timestamp: u128,
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
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
            self.value,
            self.deleted,
            self.publication_timestamp,
            self.publisher_peer_id
        )
    }
}

#[derive(Debug, Clone)]
pub struct DomoCacheSender {
    local_peer_id: Arc<str>,
    client_tx_channel: UnboundedSender<DomoEvent>,
    pub(crate) sender: mpsc::Sender<Message>,
}

type CacheTree = BTreeMap<String, BTreeMap<String, DomoCacheElement>>;

pub struct DomoCacheReceiver<T> {
    peers_caches_state: BTreeMap<String, DomoCacheStateMessage>,
    receiver: mpsc::Receiver<Message>,
    swarm_sender: SwarmSender,
    swarm_receiver: SwarmReceiver,
    cache: CacheTree,
    storage: T,
    local_peer_id: Arc<str>,
    client_rx_channel: UnboundedReceiver<DomoEvent>,
    client_tx_channel: UnboundedSender<DomoEvent>,
    send_cache_state_timer: tokio::time::Instant,
    publish_cache_counter: u8,
    last_cache_repub_timestamp: u128,
    is_persistent_cache: bool,
}

struct MessageHandler<T> {
    cache: CacheTree,
    storage: T,
    is_persistent_cache: bool,
    client_tx_channel: UnboundedSender<DomoEvent>,
    peers_caches_state: BTreeMap<String, DomoCacheStateMessage>,
    local_peer_id: Arc<str>,
    publish_cache_counter: u8,
    last_cache_repub_timestamp: u128,
    swarm_sender: SwarmSender,
}

struct MessageHandlerRef<'a, T> {
    cache: &'a CacheTree,
    _storage: &'a T,
    _is_persistent_cache: &'a bool,
    _client_tx_channel: &'a UnboundedSender<DomoEvent>,
    peers_caches_state: &'a BTreeMap<String, DomoCacheStateMessage>,
    local_peer_id: &'a Arc<str>,
    _publish_cache_counter: &'a u8,
    _last_cache_repub_timestamp: &'a u128,
    _swarm_sender: &'a SwarmSender,
}

struct MessageHandlerMut<'a, T> {
    cache: &'a mut CacheTree,
    storage: &'a mut T,
    is_persistent_cache: &'a mut bool,
    client_tx_channel: &'a UnboundedSender<DomoEvent>,
    peers_caches_state: &'a mut BTreeMap<String, DomoCacheStateMessage>,
    local_peer_id: &'a Arc<str>,
    publish_cache_counter: &'a mut u8,
    last_cache_repub_timestamp: &'a mut u128,
    swarm_sender: &'a SwarmSender,
}

#[derive(Debug)]
pub enum Message {
    PublishGossipsubFromData {
        topic: libp2p::gossipsub::IdentTopic,
        data: Vec<u8>,
        sender: MessageResponseSender<
            Result<libp2p::gossipsub::MessageId, libp2p::gossipsub::error::PublishError>,
        >,
    },
    InsertCacheElement {
        cache_element: DomoCacheElement,
        publish: bool,
        sender: MessageResponseSender<()>,
    },
    #[cfg(test)]
    FilterWithTopicName {
        topic_name: String,
        jsonpath_expr: String,
        sender: MessageResponseSender<Result<Value, String>>,
    },
    GetTopicName {
        topic_name: String,
        sender: MessageResponseSender<Result<Value, String>>,
    },
    GetAll(MessageResponseSender<Value>),
    GetTopicUuid {
        topic_name: String,
        topic_uuid: String,
        sender: MessageResponseSender<Result<Value, String>>,
    },
    PostTopicUuid {
        topic_name: String,
        topic_uuid: String,
        value: Value,
        sender: MessageResponseSender<Option<PostTopicUuidResponse>>,
    },
    DeleteTopicUuid {
        topic_name: String,
        topic_uuid: String,
        sender: MessageResponseSender<Option<DeleteTopicUuidResponse>>,
    },
    PubMessage {
        value: Value,
        sender: MessageResponseSender<()>,
    },
    OnlyInternal(OnlyInternalMessage),
}

#[derive(Debug)]
pub struct OnlyInternalMessage(OnlyInternalMessageInner);

#[derive(Debug)]
pub(crate) enum OnlyInternalMessageInner {
    ReadCacheElement {
        topic_name: String,
        topic_uuid: String,
        sender: oneshot::Sender<Option<DomoCacheElement>>,
    },
    #[cfg(test)]
    WriteWithTimestampCheck {
        topic_name: String,
        topic_uuid: String,
        elem: DomoCacheElement,
        sender: oneshot::Sender<Option<DomoCacheElement>>,
    },
    PrintPeersCache(oneshot::Sender<()>),
    Print(oneshot::Sender<()>),
    PrintCacheHash(oneshot::Sender<()>),
}

#[derive(Debug)]
pub enum MessageResponseSender<Internal> {
    Internal(oneshot::Sender<Internal>),
    Websocket {
        ws_client_id: String,
        req_id: String,
        sender: broadcast::Sender<SyncWebSocketDomoResponseMessage>,
    },
    Rest(RestResponder),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IsSynchronizedOutput {
    pub sync: bool,
    pub leader: bool,
}

const MESSAGE_HANDLER_ERROR: &str = "domo cache handler has been closed";

impl Message {
    #[inline]
    pub const fn internal() -> PreparedMessage<MessageInternal> {
        PreparedMessage(MessageInternal)
    }

    #[inline]
    pub const fn websocket(
        ws_client_id: String,
        req_id: String,
        sender: broadcast::Sender<SyncWebSocketDomoResponseMessage>,
    ) -> PreparedMessage<MessageWebsocket> {
        PreparedMessage(MessageWebsocket {
            ws_client_id,
            req_id,
            sender,
        })
    }

    #[inline]
    pub const fn rest(responder: RestResponder) -> PreparedMessage<MessageRest> {
        PreparedMessage(MessageRest(responder))
    }

    pub async fn read_cache_element(
        topic_name: String,
        topic_uuid: String,
        message_sender: &mpsc::Sender<Message>,
    ) -> Option<DomoCacheElement> {
        let (sender, receiver) = oneshot::channel();
        let message = OnlyInternalMessageInner::ReadCacheElement {
            topic_name,
            topic_uuid,
            sender,
        };
        let message = Message::OnlyInternal(OnlyInternalMessage(message));
        message_sender.send(message).await.unwrap();
        receiver.await.unwrap()
    }

    #[cfg(test)]
    pub async fn write_with_timestamp_check(
        topic_name: String,
        topic_uuid: String,
        elem: DomoCacheElement,
        message_sender: &mpsc::Sender<Message>,
    ) -> Option<DomoCacheElement> {
        let (sender, receiver) = oneshot::channel();
        let message = OnlyInternalMessageInner::WriteWithTimestampCheck {
            topic_name,
            topic_uuid,
            elem,
            sender,
        };
        let message = Message::OnlyInternal(OnlyInternalMessage(message));
        message_sender.send(message).await.unwrap();
        receiver.await.unwrap()
    }

    pub async fn print_cache_hash(message_sender: &mpsc::Sender<Message>) {
        let (sender, receiver) = oneshot::channel();
        let message = OnlyInternalMessageInner::PrintCacheHash(sender);
        let message = Message::OnlyInternal(OnlyInternalMessage(message));
        message_sender.send(message).await.unwrap();
        receiver.await.unwrap()
    }

    pub async fn print(message_sender: &mpsc::Sender<Message>) {
        let (sender, receiver) = oneshot::channel();
        let message = OnlyInternalMessageInner::Print(sender);
        let message = Message::OnlyInternal(OnlyInternalMessage(message));
        message_sender.send(message).await.unwrap();
        receiver.await.unwrap()
    }

    pub async fn print_peers_cache(message_sender: &mpsc::Sender<Message>) {
        let (sender, receiver) = oneshot::channel();
        let message = OnlyInternalMessageInner::PrintPeersCache(sender);
        let message = Message::OnlyInternal(OnlyInternalMessage(message));
        message_sender.send(message).await.unwrap();
        receiver.await.unwrap()
    }
}

pub struct PreparedMessage<T>(T);

mod sealed {
    use std::future::Future;

    use super::*;

    pub trait PreparedMessage {
        type Output<O>;
        type ReceiveFuture<'a, O: 'static>: Future<Output = Self::Output<O>> + 'a
        where
            Self: 'a;

        fn build<'a, O: 'static>(self) -> (MessageResponseSender<O>, Self::ReceiveFuture<'a, O>)
        where
            Self: 'a;
    }
}

#[non_exhaustive]
pub struct MessageInternal;

impl sealed::PreparedMessage for MessageInternal {
    type Output<O> = O;
    type ReceiveFuture<'a, O: 'static> = futures::future::Map<oneshot::Receiver<O>, fn(Result::<O, oneshot::error::RecvError>) -> O>
        where
            Self: 'a;

    #[inline]
    fn build<'a, O: 'static>(self) -> (MessageResponseSender<O>, Self::ReceiveFuture<'a, O>)
    where
        Self: 'a,
    {
        let (sender, receiver) = oneshot::channel();
        let sender = MessageResponseSender::Internal(sender);

        (
            sender,
            receiver.map(|out| out.expect(MESSAGE_HANDLER_ERROR)),
        )
    }
}

pub struct MessageWebsocket {
    ws_client_id: String,
    req_id: String,
    sender: broadcast::Sender<SyncWebSocketDomoResponseMessage>,
}

impl sealed::PreparedMessage for MessageWebsocket {
    type Output<O> = ();
    type ReceiveFuture<'a, O: 'static> = Ready<Self::Output<O>>
        where
            Self: 'a;

    fn build<'a, O: 'static>(self) -> (MessageResponseSender<O>, Self::ReceiveFuture<'a, ()>)
    where
        Self: 'a,
    {
        let Self {
            ws_client_id,
            req_id,
            sender,
        } = self;
        let sender = MessageResponseSender::Websocket {
            ws_client_id,
            req_id,
            sender,
        };
        (sender, ready(()))
    }
}

pub struct MessageRest(RestResponder);

impl sealed::PreparedMessage for MessageRest {
    type Output<O> = ();
    type ReceiveFuture<'a, O: 'static> = Ready<Self::Output<O>>    where
        Self: 'a;

    fn build<'a, O: 'static>(self) -> (MessageResponseSender<O>, Self::ReceiveFuture<'a, O>)
    where
        Self: 'a,
    {
        let sender = MessageResponseSender::Rest(self.0);
        (sender, ready(()))
    }
}

impl<T> PreparedMessage<T>
where
    T: sealed::PreparedMessage,
{
    pub async fn publish_gossipsub_from_data(
        self,
        topic: libp2p::gossipsub::IdentTopic,
        data: impl Into<Vec<u8>>,
        message_sender: &Sender<Message>,
    ) -> T::Output<Result<libp2p::gossipsub::MessageId, libp2p::gossipsub::error::PublishError>>
    {
        let data = data.into();
        let (sender, receiver) = self.0.build();
        message_sender
            .send(Message::PublishGossipsubFromData {
                topic,
                data,
                sender,
            })
            .await
            .unwrap();
        receiver.await
    }

    #[cfg(test)]
    pub async fn filter_with_topic_name(
        self,
        topic_name: String,
        jsonpath_expr: String,
        message_sender: &Sender<Message>,
    ) -> T::Output<Result<Value, String>> {
        let (sender, receiver) = self.0.build();
        message_sender
            .send(Message::FilterWithTopicName {
                topic_name,
                jsonpath_expr,
                sender,
            })
            .await
            .unwrap();
        receiver.await
    }

    pub async fn get_topic_name(
        self,
        topic_name: impl Into<String>,
        message_sender: &Sender<Message>,
    ) -> T::Output<Result<Value, String>> {
        let topic_name = topic_name.into();
        let (sender, receiver) = self.0.build();
        message_sender
            .send(Message::GetTopicName { topic_name, sender })
            .await
            .unwrap();
        receiver.await
    }

    pub async fn get_all(self, message_sender: &Sender<Message>) -> T::Output<Value> {
        let (sender, receiver) = self.0.build();
        message_sender.send(Message::GetAll(sender)).await.unwrap();
        receiver.await
    }

    pub async fn insert_cache_element(
        self,
        cache_element: DomoCacheElement,
        publish: bool,
        message_sender: &Sender<Message>,
    ) -> T::Output<()> {
        let (sender, receiver) = self.0.build();
        message_sender
            .send(Message::InsertCacheElement {
                cache_element,
                publish,
                sender,
            })
            .await
            .unwrap();
        receiver.await
    }

    pub async fn get_topic_uuid(
        self,
        topic_name: String,
        topic_uuid: String,
        message_sender: &Sender<Message>,
    ) -> T::Output<Result<Value, String>> {
        let (sender, receiver) = self.0.build();
        message_sender
            .send(Message::GetTopicUuid {
                topic_name,
                topic_uuid,
                sender,
            })
            .await
            .unwrap();
        receiver.await
    }

    pub async fn post_topic_uuid(
        self,
        topic_name: String,
        topic_uuid: String,
        value: Value,
        message_sender: &Sender<Message>,
    ) -> T::Output<Option<PostTopicUuidResponse>> {
        let (sender, receiver) = self.0.build();
        message_sender
            .send(Message::PostTopicUuid {
                topic_name,
                topic_uuid,
                value,
                sender,
            })
            .await
            .unwrap();
        receiver.await
    }

    pub async fn delete_topic_uuid(
        self,
        topic_name: String,
        topic_uuid: String,
        message_sender: &Sender<Message>,
    ) -> T::Output<Option<DeleteTopicUuidResponse>> {
        let (sender, receiver) = self.0.build();
        message_sender
            .send(Message::DeleteTopicUuid {
                topic_name,
                topic_uuid,
                sender,
            })
            .await
            .unwrap();
        receiver.await
    }

    pub async fn pub_message(
        self,
        value: Value,
        message_sender: &Sender<Message>,
    ) -> T::Output<()> {
        let (sender, receiver) = self.0.build();
        message_sender
            .send(Message::PubMessage { value, sender })
            .await
            .unwrap();
        receiver.await
    }
}

impl<T> Hash for DomoCacheReceiver<T> {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.message_handler().hash(state)
    }
}

impl<'a, T> Hash for MessageHandlerRef<'a, T> {
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

impl<'a, T> Hash for MessageHandlerMut<'a, T> {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.to_ref().hash(state)
    }
}

pub async fn channel<T: DomoPersistentStorage>(
    is_persistent_cache: bool,
    storage: T,
    shared_key: String,
    local_key_pair: Keypair,
    loopback_only: bool,
    channel_size: Option<usize>,
) -> (DomoCacheSender, DomoCacheReceiver<T>) {
    let swarm = crate::domolibp2p::start(shared_key, local_key_pair, loopback_only)
        .await
        .unwrap();
    let peer_id = Arc::from(swarm.local_peer_id().to_string());

    // FIXME: allow customization of this buffer
    let (swarm_sender, swarm_receiver) = swarm::channel(swarm, 32);

    let (client_tx_channel, client_rx_channel) = mpsc::unbounded_channel();

    let send_cache_state_timer: tokio::time::Instant =
        tokio::time::Instant::now() + Duration::from_secs(u64::from(SEND_CACHE_HASH_PERIOD));

    let (sender, receiver) = mpsc::channel(channel_size.unwrap_or(32));

    let c = DomoCacheSender {
        local_peer_id: Arc::clone(&peer_id),
        client_tx_channel: client_tx_channel.clone(),
        sender,
    };

    let mut handler = DomoCacheReceiver {
        swarm_receiver,
        swarm_sender,
        peers_caches_state: BTreeMap::new(),
        receiver,
        storage,
        cache: BTreeMap::new(),
        local_peer_id: peer_id,
        client_rx_channel,
        client_tx_channel,
        send_cache_state_timer,
        publish_cache_counter: 4,
        last_cache_repub_timestamp: 0,
        is_persistent_cache,
    };

    // Populate the cache with the sqlite contents
    let ret = handler.storage.get_all_elements();
    for elem in ret.into_iter() {
        write_cache(&mut handler.cache, elem);
    }

    (c, handler)
}

impl DomoCacheSender {
    #[cfg(test)]
    pub async fn filter_with_topic_name(
        &self,
        topic_name: impl Into<String>,
        jsonpath_expr: impl Into<String>,
    ) -> Result<Value, String> {
        let topic_name = topic_name.into();
        let jsonpath_expr = jsonpath_expr.into();

        Message::internal()
            .filter_with_topic_name(topic_name, jsonpath_expr, &self.sender)
            .await
    }

    pub async fn get_topic_uuid(
        &self,
        topic_name: &str,
        topic_uuid: &str,
    ) -> Result<Value, String> {
        Message::internal()
            .get_topic_uuid(topic_name.to_owned(), topic_uuid.to_owned(), &self.sender)
            .await
    }

    pub async fn pub_value(&self, value: Value) {
        let topic = Topic::new("domo-volatile-data");

        let m = serde_json::to_string(&value).unwrap();

        if let Err(e) = Message::internal()
            .publish_gossipsub_from_data(topic.clone(), m, &self.sender)
            .await
        {
            log::info!("Publish error: {:?}", e);
        }

        // signal a volatile pub by part of clients
        let ev = DomoEvent::VolatileData(value);
        self.client_tx_channel.send(ev).unwrap();
    }

    pub async fn gossip_pub(&self, mut m: DomoCacheElement, republished: bool) {
        let topic = Topic::new("domo-persistent-data");

        if republished {
            m.republication_timestamp = utils::get_epoch_ms();
        } else {
            m.republication_timestamp = 0;
        }

        let m2 = serde_json::to_string(&m).unwrap();

        if let Err(e) = Message::internal()
            .publish_gossipsub_from_data(topic.clone(), m2, &self.sender)
            .await
        {
            log::info!("Publish error: {e:?}");
        }
        if !republished {
            // signal a volatile pub by part of clients
            let ev = DomoEvent::PersistentData(m);
            self.client_tx_channel.send(ev).unwrap();
        }
    }

    pub async fn delete_value(&self, topic_name: &str, topic_uuid: &str) {
        let timest = utils::get_epoch_ms();
        let elem = DomoCacheElement {
            topic_name: String::from(topic_name),
            topic_uuid: String::from(topic_uuid),
            publication_timestamp: timest,
            value: Value::Null,
            deleted: true,
            publisher_peer_id: self.local_peer_id.to_string(),
            republication_timestamp: 0,
        };

        self.insert_cache_element(elem, true).await;
    }

    // metodo chiamato dall'applicazione, metto in cache e pubblico
    pub async fn write_value(&self, topic_name: &str, topic_uuid: &str, value: Value) {
        self.insert_cache_element(
            create_cache_element_to_write(
                topic_name,
                topic_uuid,
                value,
                self.local_peer_id.to_string(),
            ),
            true,
        )
        .await;
    }

    async fn insert_cache_element(&self, cache_element: DomoCacheElement, publish: bool) {
        Message::internal()
            .insert_cache_element(cache_element, publish, &self.sender)
            .await;
    }

    #[cfg(test)]
    async fn write_with_timestamp_check(
        &self,
        topic_name: impl Into<String>,
        topic_uuid: impl Into<String>,
        elem: DomoCacheElement,
    ) -> Option<DomoCacheElement> {
        let topic_name = topic_name.into();
        let topic_uuid = topic_uuid.into();

        Message::write_with_timestamp_check(topic_name, topic_uuid, elem, &self.sender).await
    }

    #[cfg(test)]
    async fn read_cache_element(
        &self,
        topic_name: impl Into<String>,
        topic_uuid: impl Into<String>,
    ) -> Option<DomoCacheElement> {
        let topic_name = topic_name.into();
        let topic_uuid = topic_uuid.into();
        Message::read_cache_element(topic_name, topic_uuid, &self.sender).await
    }

    pub async fn get_all(&self) -> Value {
        Message::internal().get_all(&self.sender).await
    }

    pub async fn get_topic_name(&self, topic_name: impl Into<String>) -> Result<Value, String> {
        let topic_name = topic_name.into();
        Message::internal()
            .get_topic_name(topic_name, &self.sender)
            .await
    }

    #[inline]
    pub fn local_peer_id(&self) -> &str {
        &self.local_peer_id
    }

    pub async fn print_peers_cache(&self) {
        Message::print_peers_cache(&self.sender).await;
    }

    pub async fn print(&self) {
        Message::print(&self.sender).await;
    }

    pub async fn print_cache_hash(&self) {
        Message::print_cache_hash(&self.sender).await;
    }
}

fn create_cache_element_to_write(
    topic_name: &str,
    topic_uuid: &str,
    value: Value,
    publisher_peer_id: String,
) -> DomoCacheElement {
    let timest = utils::get_epoch_ms();
    DomoCacheElement {
        topic_name: String::from(topic_name),
        topic_uuid: String::from(topic_uuid),
        publication_timestamp: timest,
        value,
        deleted: false,
        publisher_peer_id,
        republication_timestamp: 0,
    }
}

impl<T> DomoCacheReceiver<T> {
    #[inline]
    fn message_handler_mut(&mut self) -> MessageHandlerMut<'_, T> {
        self.split_mut().3
    }

    #[inline]
    fn message_handler(&self) -> MessageHandlerRef<'_, T> {
        let Self {
            peers_caches_state,
            swarm_sender,
            cache,
            storage,
            client_tx_channel,
            is_persistent_cache,
            local_peer_id,
            publish_cache_counter,
            last_cache_repub_timestamp,
            ..
        } = self;

        MessageHandlerRef {
            cache,
            _storage: storage,
            _is_persistent_cache: is_persistent_cache,
            _client_tx_channel: client_tx_channel,
            peers_caches_state,
            local_peer_id,
            _publish_cache_counter: publish_cache_counter,
            _last_cache_repub_timestamp: last_cache_repub_timestamp,
            _swarm_sender: swarm_sender,
        }
    }

    #[inline]
    fn split(
        self,
    ) -> (
        UnboundedReceiver<DomoEvent>,
        mpsc::Receiver<Message>,
        SwarmReceiver,
        MessageHandler<T>,
    ) {
        let Self {
            peers_caches_state,
            receiver,
            swarm_receiver,
            cache,
            storage,
            client_rx_channel,
            client_tx_channel,
            is_persistent_cache,
            local_peer_id,
            publish_cache_counter,
            last_cache_repub_timestamp,
            swarm_sender,
            ..
        } = self;

        let message_handler = MessageHandler {
            cache,
            storage,
            is_persistent_cache,
            client_tx_channel,
            peers_caches_state,
            local_peer_id,
            publish_cache_counter,
            last_cache_repub_timestamp,
            swarm_sender,
        };

        (client_rx_channel, receiver, swarm_receiver, message_handler)
    }

    #[inline]
    fn split_mut(
        &mut self,
    ) -> (
        &mut UnboundedReceiver<DomoEvent>,
        &mut mpsc::Receiver<Message>,
        &mut SwarmReceiver,
        MessageHandlerMut<'_, T>,
    ) {
        let Self {
            peers_caches_state,
            receiver,
            swarm_receiver,
            cache,
            storage,
            client_rx_channel,
            client_tx_channel,
            is_persistent_cache,
            local_peer_id,
            publish_cache_counter,
            last_cache_repub_timestamp,
            swarm_sender,
            ..
        } = self;

        let message_handler = MessageHandlerMut {
            cache,
            storage,
            is_persistent_cache,
            client_tx_channel,
            peers_caches_state,
            local_peer_id,
            publish_cache_counter,
            last_cache_repub_timestamp,
            swarm_sender,
        };

        (client_rx_channel, receiver, swarm_receiver, message_handler)
    }
}

type DomoSwarmEvent =
    libp2p::swarm::SwarmEvent<domolibp2p::OutEvent, EitherError<Void, GossipsubHandlerError>>;

impl<T: DomoPersistentStorage> DomoCacheReceiver<T> {
    pub fn get_cache_hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.hash(&mut s);
        s.finish()
    }

    pub fn get_all(&self) -> Value {
        let s = r#"[]"#;
        let mut ret: Value = serde_json::from_str(s).unwrap();

        for (topic_name, topic_name_map) in self.cache.iter() {
            for (topic_uuid, cache_element) in topic_name_map.iter() {
                if !cache_element.deleted {
                    let val = serde_json::json!({
                        "topic_name": topic_name.clone(),
                        "topic_uuid": topic_uuid.clone(),
                        "value": cache_element.value.clone()
                    });
                    ret.as_array_mut().unwrap().push(val);
                }
            }
        }
        ret
    }

    pub fn get_topic_name(&self, topic_name: &str) -> Result<Value, String> {
        let s = r#"[]"#;
        let mut ret: Value = serde_json::from_str(s).unwrap();

        match self.cache.get(topic_name) {
            None => Ok(serde_json::json!([])),
            Some(topic_name_map) => {
                for (topic_uuid, cache_element) in topic_name_map.iter() {
                    if !cache_element.deleted {
                        let val = serde_json::json!({
                            "topic_name": topic_name.to_owned(),
                            "topic_uuid": topic_uuid.to_owned(),
                            "value": cache_element.value.clone()
                        });
                        ret.as_array_mut().unwrap().push(val);
                    }
                }
                Ok(ret)
            }
        }
    }

    pub async fn write_value(&mut self, topic_name: &str, topic_uuid: &str, value: Value) {
        let elem = create_cache_element_to_write(
            topic_name,
            topic_uuid,
            value,
            self.local_peer_id.to_string(),
        );

        if let Some(event) = insert_cache_element(
            &mut self.cache,
            &mut self.storage,
            &self.swarm_sender,
            elem,
            self.is_persistent_cache,
            true,
        )
        .await
        {
            self.client_tx_channel.send(event).unwrap();
        }
    }

    pub fn get_topic_uuid(&self, topic_name: &str, topic_uuid: &str) -> Result<Value, String> {
        let ret = self.read_cache_element(topic_name, topic_uuid);
        match ret {
            None => Ok(serde_json::json!({})),
            Some(cache_element) => Ok(serde_json::json!({
                "topic_name": topic_name.to_owned(),
                "topic_uuid": topic_uuid.to_owned(),
                "value": cache_element.value
            })),
        }
    }

    fn read_cache_element(&self, topic_name: &str, topic_uuid: &str) -> Option<DomoCacheElement> {
        self.cache
            .get(topic_name)
            .and_then(|topic_map| topic_map.get(topic_uuid))
            .filter(|element| element.deleted.not())
            .map(Clone::clone)
    }

    pub async fn delete_value(&mut self, topic_name: &str, topic_uuid: &str) {
        self.message_handler_mut()
            .delete_value(topic_name, topic_uuid)
            .await;
    }

    pub async fn pub_value(&self, value: Value) {
        pub_message(&self.swarm_sender, &self.client_tx_channel, value).await
    }

    pub fn into_stream(self) -> impl Stream<Item = std::result::Result<DomoEvent, Box<dyn Error>>>
    where
        T: 'static,
    {
        // TODO: benchmark whether allocating could improve overall performances.
        #[allow(clippy::large_enum_variant)]
        enum Received {
            Message(Message),
            Timeout,
            SwarmMessage(InternalSwarmMessage),
        }

        let send_cache_state_timer = self.send_cache_state_timer;
        let (client_rx_channel, receiver, swarm_receiver, message_handler) = self.split();

        let client_rx_stream = UnboundedReceiverStream::new(client_rx_channel).map(Ok);

        let receiver_stream = ReceiverStream::new(receiver).map(Received::Message);
        let timeout_stream =
            futures_util::stream::once(tokio::time::sleep_until(send_cache_state_timer))
                .chain(
                    IntervalStream::new(tokio::time::interval(Duration::from_secs(u64::from(
                        SEND_CACHE_HASH_PERIOD,
                    ))))
                    .map(|_| ()),
                )
                .map(|()| Received::Timeout);

        let (swarm_managed_event_sender, swarm_managed_event_receiver) = mpsc::channel(8);
        let swarm_sender = message_handler.swarm_sender.clone();
        let swarm_stream = swarm_receiver.into_stream().then(move |event| {
            handle_swarm_event(
                event,
                swarm_managed_event_sender.clone(),
                swarm_sender.clone(),
            )
        });

        let internal_swarm_stream =
            ReceiverStream::new(swarm_managed_event_receiver).map(Received::SwarmMessage);

        // The reason this should work flawlessly is not trivial, readers are encouraged to prove
        // me wrong.
        //
        // We are merging the output of three streams, which needs to be processed further. The
        // output is actually an enum of three possible variants, as a consequence of the three
        // different streams. The reason behind this approach is that the `message_handler`
        // modifies its internal state while processing each of the variants, funnelling all the
        // possible message in one logic flow allows us to avoid sharing the message handler across
        // different futures without a mutex. Wait... we ARE using a mutex, why???
        //
        // First of all, this is suspiciously a _sync_ mutex, which should not be used in these
        // contexts. We want this to happen:
        // 1. Merged streams produce an output
        // 2. The output is pattern-matched
        // 3. Different internal values are passed to the message handler, possibly suspending.
        // 4. For some specific cases of swarm events, the output is forwarded downstream,
        //    otherwise the merged stream is free to be polled again.
        // 5. At some point, the merged stream can be polled again and the loop restarts.
        //
        // As you can see, there is no actual concurrency in the handling of the output, it's just
        // an async FIFO. Therefore, the mutex should **never** lock, which also means that we can
        // use a sync lock instead of an async one (and hopefully improve performances a bit). But
        // then why are we using a mutex?
        //
        // In theory, all this mess can be replaced by a manually written stream that internally
        // stores an enum with these variants:
        // - the message handler
        // - a future that resolves to the message handler
        // - a future that resolves to a tuple with a `ControlFlow` and the message handler
        //
        // This pattern is conceptually pretty simple: the future resulting from the call to
        // `.handle_message` or `.handle_internal_swarm_message` brings the lifetime of
        // `message_handler`, and in order to avoid writing a self-referential stream, it is
        // possible to extract the message handler (yes, we need an "invalid" variant for the enum)
        // and use the compiler superpowers in order to build a future that eats the message
        // handler and spits out the output of the async function and the message handler itself.
        // This future is, like most of the futures, self-referential, but we don't need to write
        // down nitty-gritty unsafe code. But:
        // 1. We need to box these futures until we got type alias impl traits (TAITs)
        // 2. The implementation is still not trivial, and it must be maintained
        //
        // Therefore, here's why we are using a (hopefully) never-locking mutex. If you think that
        // any of our assumption is wrong, please open an issue providing a way to reproduce the
        // unexpected behavior.
        let message_handler = Arc::new(std::sync::Mutex::new(message_handler));

        let handler_stream = (receiver_stream, timeout_stream, internal_swarm_stream)
            .merge()
            .filter_map(move |received| {
                let message_handler = Arc::clone(&message_handler);

                // See previous explanation why this lock should be fine even across await
                // points.
                #[allow(clippy::await_holding_lock)]
                async move {
                    let mut message_handler = message_handler.try_lock().unwrap();
                    match received {
                        Received::Message(message) => {
                            message_handler.handle_message(message).await;
                            None
                        }

                        Received::Timeout => {
                            message_handler.send_cache_state().await;
                            None
                        }

                        Received::SwarmMessage(swarm_message) => {
                            match message_handler
                                .handle_internal_swarm_message(swarm_message)
                                .await
                            {
                                ControlFlow::Break(out) => Some(out),
                                ControlFlow::Continue(()) => None,
                            }
                        }
                    }
                }
            });

        (
            (client_rx_stream, handler_stream).merge().map(Some),
            swarm_stream.map(|()| None),
        )
            .merge()
            .filter_map(ready)
    }
}

impl<T> MessageHandlerMut<'_, T> {
    #[inline]
    fn to_ref(&self) -> MessageHandlerRef<'_, T> {
        let Self {
            cache,
            storage,
            is_persistent_cache,
            client_tx_channel,
            peers_caches_state,
            local_peer_id,
            publish_cache_counter,
            last_cache_repub_timestamp,
            swarm_sender,
        } = self;

        MessageHandlerRef {
            cache,
            _storage: storage,
            _is_persistent_cache: is_persistent_cache,
            _client_tx_channel: client_tx_channel,
            peers_caches_state,
            local_peer_id,
            _publish_cache_counter: publish_cache_counter,
            _last_cache_repub_timestamp: last_cache_repub_timestamp,
            _swarm_sender: swarm_sender,
        }
    }
}

impl<T: DomoPersistentStorage> MessageHandlerMut<'_, T> {
    async fn handle_message(&mut self, message: Message) {
        #[inline]
        fn handle_internal<T>(sender: oneshot::Sender<T>, value: T, message_ty: &str) {
            if sender.send(value).is_err() {
                log::warn!(
                    "internal oneshot channel to return result of message {} is unexpectedly closed",
                    message_ty,
                );
            }
        }

        #[inline]
        fn handle_rest<T>(sender: RestResponder, value: T, message_ty: &str)
        where
            T: Serialize,
        {
            if sender
                .send(Ok(serde_json::to_value(value).unwrap()))
                .is_err()
            {
                log::warn!(
                    "rest channel to return result of message {} is unexpectedly closed",
                    message_ty,
                );
            }
        }

        #[inline]
        fn handle_return_no_response<T>(
            sender: MessageResponseSender<T>,
            value: T,
            message_ty: &str,
        ) {
            match sender {
                MessageResponseSender::Internal(sender) => {
                    handle_internal(sender, value, message_ty)
                }
                MessageResponseSender::Rest(responder) => {
                    handle_rest(responder, Ok::<_, String>(()), message_ty)
                }
                MessageResponseSender::Websocket { .. } => {}
            }
        }

        #[inline]
        fn handle_return_with_response<T: Serialize>(
            sender: MessageResponseSender<T>,
            value: T,
            message_ty: &str,
        ) {
            match sender {
                MessageResponseSender::Internal(sender) => {
                    handle_internal(sender, value, message_ty)
                }
                MessageResponseSender::Websocket {
                    ws_client_id,
                    req_id,
                    sender,
                } => {
                    if sender
                        .send(SyncWebSocketDomoResponseMessage {
                            ws_client_id,
                            req_id,
                            response: serde_json::to_string(&value).unwrap(),
                        })
                        .is_err()
                    {
                        log::warn!(
                            "websocket channel to return result of message {} is unexpectedly \
                            closed",
                            message_ty,
                        );
                    }
                }
                MessageResponseSender::Rest(sender) => handle_rest(sender, value, message_ty),
            }
        }

        match message {
            Message::PublishGossipsubFromData {
                topic,
                data,
                sender,
            } => match publish_gossipsub(self.swarm_sender, topic, data).await {
                Ok(value) => {
                    handle_return_no_response(sender, Ok(value), "PublishGossipsubFromData")
                }
                Err(SendError::Swarm(err)) => {
                    handle_return_no_response(sender, Err(err), "PublishGossipsubFromData")
                }
                Err(SendError::Channel(err)) => panic!("{err}"),
            },
            Message::InsertCacheElement {
                cache_element,
                publish,
                sender,
            } => {
                if let Some(event) = insert_cache_element(
                    self.cache,
                    self.storage,
                    self.swarm_sender,
                    cache_element,
                    *self.is_persistent_cache,
                    publish,
                )
                .await
                {
                    self.client_tx_channel.send(event).unwrap()
                }

                handle_return_no_response(sender, (), "InsertCacheElement");
            }
            #[cfg(test)]
            Message::FilterWithTopicName {
                topic_name,
                jsonpath_expr,
                sender,
            } => {
                let result = match self.cache.get(&topic_name) {
                    None => Ok(serde_json::json!([])),
                    Some(topic_map) => 'result: {
                        let mut ret = serde_json::json!([]);
                        for (_topic_uuid, topic_value) in topic_map.iter() {
                            let val = serde_json::json!(
                                {
                                    "value": [topic_value.value]
                                }
                            );

                            let result = jsonpath_lib::select(&val, &jsonpath_expr);

                            match result {
                                Ok(res) => {
                                    for r in res {
                                        ret.as_array_mut().unwrap().push(r.clone());
                                    }
                                }
                                Err(e) => break 'result Err(e.to_string()),
                            };
                        }

                        Ok(ret)
                    }
                };

                handle_return_with_response(sender, result, "FilterWithTopicName");
            }
            Message::GetTopicName { topic_name, sender } => {
                let s = r#"[]"#;
                let mut ret: Value = serde_json::from_str(s).unwrap();

                let result = match self.cache.get(&topic_name) {
                    None => Ok(serde_json::json!([])),
                    Some(topic_name_map) => {
                        for (topic_uuid, cache_element) in topic_name_map.iter() {
                            if !cache_element.deleted {
                                let val = serde_json::json!({
                                    "topic_name": topic_name.to_owned(),
                                    "topic_uuid": topic_uuid.to_owned(),
                                    "value": cache_element.value.clone()
                                });
                                ret.as_array_mut().unwrap().push(val);
                            }
                        }
                        Ok(ret)
                    }
                };

                handle_return_with_response(sender, result, "GetTopicName");
            }
            Message::GetTopicUuid {
                topic_name,
                topic_uuid,
                sender,
            } => {
                let out = self
                    .cache
                    .get(&topic_name)
                    .and_then(|topic_map| topic_map.get(&topic_uuid))
                    .filter(|element| element.deleted.not())
                    .map(|element| {
                        serde_json::json!({
                            "topic_name": topic_name.to_owned(),
                            "topic_uuid": topic_uuid.to_owned(),
                            "value": element.value
                        })
                    })
                    .unwrap_or_default();

                handle_return_with_response(sender, Ok(out), "GetTopicUuid");
            }
            Message::PostTopicUuid {
                topic_name,
                topic_uuid,
                value,
                sender,
            } => {
                let elem = create_cache_element_to_write(
                    &topic_name,
                    &topic_uuid,
                    value,
                    self.local_peer_id.to_string(),
                );

                let event = insert_cache_element(
                    self.cache,
                    self.storage,
                    self.swarm_sender,
                    elem,
                    *self.is_persistent_cache,
                    true,
                )
                .await;

                if let Some(event) = &event {
                    self.client_tx_channel.send(event.clone()).unwrap();
                }

                let out = event.and_then(|event| match event {
                    DomoEvent::None => None,
                    DomoEvent::VolatileData(value) => {
                        Some(PostTopicUuidResponse::Volatile { value })
                    }
                    DomoEvent::PersistentData(DomoCacheElement {
                        topic_name,
                        topic_uuid,
                        value,
                        deleted,
                        ..
                    }) => Some(PostTopicUuidResponse::Persistent {
                        value,
                        topic_name,
                        topic_uuid,
                        deleted,
                    }),
                });
                handle_return_with_response(sender, out, "PostTopicUuid");
            }
            Message::GetAll(sender) => {
                let s = r#"[]"#;
                let mut ret: Value = serde_json::from_str(s).unwrap();

                for (topic_name, topic_name_map) in self.cache.iter() {
                    for (topic_uuid, cache_element) in topic_name_map.iter() {
                        if !cache_element.deleted {
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

                handle_return_with_response(sender, ret, "GetAll");
            }
            Message::DeleteTopicUuid {
                topic_name,
                topic_uuid,
                sender,
            } => {
                let elem = self.delete_value(&topic_name, &topic_uuid).await;
                handle_return_with_response(sender, elem, "DeleteTopicUuid")
            }

            Message::PubMessage { value, sender } => {
                pub_message(self.swarm_sender, self.client_tx_channel, value).await;
                handle_return_no_response(sender, (), "PubMessage")
            }

            Message::OnlyInternal(OnlyInternalMessage(message)) => match message {
                OnlyInternalMessageInner::ReadCacheElement {
                    topic_name,
                    topic_uuid,
                    sender,
                } => {
                    let out = self
                        .cache
                        .get(&topic_name)
                        .and_then(|topic_map| topic_map.get(&topic_uuid))
                        .filter(|element| element.deleted.not())
                        .map(Clone::clone);

                    handle_internal(sender, out, "ReadCacheElement");
                }

                #[cfg(test)]
                OnlyInternalMessageInner::WriteWithTimestampCheck {
                    topic_name,
                    topic_uuid,
                    elem,
                    sender,
                } => {
                    let cache_element = self
                        .write_with_timestamp_check(&topic_name, &topic_uuid, elem)
                        .await;
                    handle_internal(sender, cache_element, "WriteWithTimestampCheck");
                }

                OnlyInternalMessageInner::PrintPeersCache(sender) => {
                    self.print_peers_cache();
                    handle_internal(sender, (), "PrintPeersCache");
                }

                OnlyInternalMessageInner::Print(sender) => {
                    self.print();
                    handle_internal(sender, (), "Print");
                }

                OnlyInternalMessageInner::PrintCacheHash(sender) => {
                    self.print_cache_hash();
                    handle_internal(sender, (), "PrintCacheHash");
                }
            },
        }
    }

    async fn handle_internal_swarm_message(
        &mut self,
        swarm_message: InternalSwarmMessage,
    ) -> ControlFlow<Result<DomoEvent, Box<dyn std::error::Error>>> {
        match swarm_message {
            InternalSwarmMessage::HandlePersistentMessage(message) => ControlFlow::Break(
                self.handle_persistent_message_data(&String::from_utf8_lossy(&message))
                    .await,
            ),
            InternalSwarmMessage::HandleConfigData(config_data) => {
                self.handle_config_data(&String::from_utf8_lossy(&config_data))
                    .await;
                ControlFlow::Continue(())
            }
            InternalSwarmMessage::VolatileData(data) => ControlFlow::Break(
                serde_json::from_slice(&data)
                    .map(DomoEvent::VolatileData)
                    .map_err(|x| x.into()),
            ),
        }
    }

    async fn write_with_timestamp_check(
        &mut self,
        topic_name: &str,
        topic_uuid: &str,
        elem: DomoCacheElement,
    ) -> Option<DomoCacheElement> {
        let cache_element = self
            .cache
            .get(topic_name)
            .and_then(|topic_map| topic_map.get(topic_uuid));

        // Cannot use and_then, at least for now, because borrowck is not happy about the
        // reference to `self.cache` for the second time.
        match cache_element {
            Some(cache_element) => {
                let out = (elem.publication_timestamp <= cache_element.publication_timestamp)
                    .then(|| cache_element.clone());

                if out.is_none() {
                    if let Some(event) = insert_cache_element(
                        self.cache,
                        self.storage,
                        self.swarm_sender,
                        elem,
                        *self.is_persistent_cache,
                        false,
                    )
                    .await
                    {
                        self.client_tx_channel.send(event).unwrap();
                    }
                }

                out
            }
            None => None,
        }
    }

    async fn send_cache_state(&mut self) {
        let topic = Topic::new("domo-config");

        let data = DomoCacheStateMessage {
            peer_id: self.local_peer_id.to_string(),
            cache_hash: self.get_cache_hash(),
            publication_timestamp: crate::utils::get_epoch_ms(),
        };

        let data = serde_json::to_string(&data).unwrap();

        if let Err(e) = self
            .swarm_sender
            .publish_gossipsub_with_return(topic, data.into())
            .await
        {
            log::info!("Publish error: {e:?}");
        } else {
            log::info!("Published cache hash");
        }

        if *self.publish_cache_counter == 1 {
            *self.publish_cache_counter = 4;
            self.check_caches_desynchronization().await;
        } else {
            *self.publish_cache_counter -= 1;
        }
    }

    async fn handle_config_data(&mut self, message: &str) {
        log::info!("Received cache message, check caches ...");
        let message = serde_json::from_str(message).unwrap();
        self.insert_cache_state(message);
        self.check_caches_desynchronization().await;
    }

    #[inline]
    fn insert_cache_state(&mut self, message: DomoCacheStateMessage) {
        self.peers_caches_state
            .insert(message.peer_id.clone(), message);
    }

    async fn check_caches_desynchronization(&mut self) {
        let local_hash = self.get_cache_hash();
        let (sync, leader) = self
            .to_ref()
            .is_synchronized(local_hash, self.peers_caches_state);

        if !sync {
            log::info!("Caches are not synchronized");
            if leader {
                log::info!("Publishing my cache since I am the leader for the hash");
                if *self.last_cache_repub_timestamp
                    < (utils::get_epoch_ms() - 1000 * u128::from(SEND_CACHE_HASH_PERIOD))
                {
                    self.publish_cache().await;
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

    async fn handle_persistent_message_data(
        &mut self,
        message: &str,
    ) -> std::result::Result<DomoEvent, Box<dyn Error>> {
        let mut m: DomoCacheElement = serde_json::from_str(message)?;

        // rimetto a 0 il republication timestamp altrimenti cambia hash
        m.republication_timestamp = 0;

        let topic_name = m.topic_name.clone();
        let topic_uuid = m.topic_uuid.clone();

        let ret = self
            .write_with_timestamp_check(&topic_name, &topic_uuid, m.clone())
            .await;

        match ret {
            None => {
                log::info!("New message received");
                // since a new message arrived, we invalidate peers cache states
                self.peers_caches_state.clear();
                Ok(DomoEvent::PersistentData(m))
            }
            _ => {
                log::info!("Old message received");
                Ok(DomoEvent::None)
            }
        }
    }

    #[inline]
    fn print_peers_cache(&self) {
        self.to_ref().print_peers_cache();
    }

    #[inline]
    fn print(&self) {
        self.to_ref().print();
    }

    #[inline]
    fn print_cache_hash(&self) {
        self.to_ref().print_cache_hash();
    }

    #[inline]
    fn get_cache_hash(&self) -> u64 {
        self.to_ref().get_cache_hash()
    }

    async fn publish_cache(&mut self) {
        let mut cache_elements = vec![];

        for (_, topic_name_map) in self.cache.iter() {
            for (_, cache_element) in topic_name_map.iter() {
                cache_elements.push(cache_element.clone());
            }
        }

        stream::iter(cache_elements)
            .filter_map(|elem| gossip_pub(self.swarm_sender, elem, true))
            .for_each(|event| {
                self.client_tx_channel.send(event).unwrap();
                ready(())
            })
            .await;

        *self.last_cache_repub_timestamp = utils::get_epoch_ms();
    }

    async fn delete_value(
        &mut self,
        topic_name: &str,
        topic_uuid: &str,
    ) -> Option<DeleteTopicUuidResponse> {
        let timest = utils::get_epoch_ms();
        let elem = DomoCacheElement {
            topic_name: String::from(topic_name),
            topic_uuid: String::from(topic_uuid),
            publication_timestamp: timest,
            value: Value::Null,
            deleted: true,
            publisher_peer_id: self.local_peer_id.to_string(),
            republication_timestamp: 0,
        };

        let event = insert_cache_element(
            self.cache,
            self.storage,
            self.swarm_sender,
            elem,
            *self.is_persistent_cache,
            true,
        )
        .await;

        event.map(|event| {
            self.client_tx_channel.send(event).unwrap();
            DeleteTopicUuidResponse {
                value: Value::Null,
                topic_name: topic_name.to_owned(),
                topic_uuid: topic_uuid.to_owned(),
                deleted: true,
            }
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PostTopicUuidResponse {
    Volatile {
        value: Value,
    },
    Persistent {
        value: Value,
        topic_name: String,
        topic_uuid: String,
        deleted: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteTopicUuidResponse {
    value: Value,
    topic_name: String,
    topic_uuid: String,
    deleted: bool,
}

#[derive(Debug)]
enum InternalSwarmMessage {
    HandlePersistentMessage(Vec<u8>),
    HandleConfigData(Vec<u8>),
    VolatileData(Vec<u8>),
}

async fn handle_swarm_event(
    event: DomoSwarmEvent,
    sender: mpsc::Sender<InternalSwarmMessage>,
    swarm_sender: SwarmSender,
) {
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
            libp2p::gossipsub::GossipsubEvent::Message {
                propagation_source: _peer_id,
                message_id: _id,
                message,
            },
        )) => match message.topic.to_string().as_str() {
            "domo-persistent-data" => {
                sender
                    .send(InternalSwarmMessage::HandlePersistentMessage(message.data))
                    .await
                    .unwrap();
            }
            "domo-config" => {
                sender
                    .send(InternalSwarmMessage::HandleConfigData(message.data))
                    .await
                    .unwrap();
            }
            "domo-volatile-data" => {
                sender
                    .send(InternalSwarmMessage::VolatileData(message.data))
                    .await
                    .unwrap();
            }
            _ => {
                log::info!("Not able to recognize message");
            }
        },
        SwarmEvent::Behaviour(crate::domolibp2p::OutEvent::Mdns(libp2p::mdns::Event::Expired(
            list,
        ))) => {
            let peers = list.into_iter().map(|(peer, _)| peer).collect();
            swarm_sender.publish_gossipsub_peers(peers).await.unwrap()
        }
        _ => {}
    }
}

impl<T: DomoPersistentStorage> MessageHandlerRef<'_, T> {
    fn print_peers_cache(&self) {
        for (peer_id, peer_data) in self.peers_caches_state.iter() {
            println!(
                "Peer {}, HASH: {}, TIMESTAMP: {}",
                peer_id, peer_data.cache_hash, peer_data.publication_timestamp
            );
        }
    }

    fn print(&self) {
        for (topic_name, topic_name_map) in self.cache.iter() {
            let mut first = true;

            for (_, value) in topic_name_map.iter() {
                if !value.deleted {
                    if first {
                        println!("TopicName {topic_name}");
                        first = false;
                    }
                    println!("{value}");
                }
            }
        }
    }

    fn print_cache_hash(&self) {
        println!("Hash {}", self.get_cache_hash())
    }

    fn get_cache_hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.hash(&mut s);
        s.finish()
    }

    /// Returns a tuple (is_synchronized, is_hash_leader)
    fn is_synchronized(
        &self,
        local_hash: u64,
        peers_caches_state: &BTreeMap<String, DomoCacheStateMessage>,
    ) -> (bool, bool) {
        // If there are hashes different from the current node,
        // the state is not consistent. Then we need to check
        // whether we are the leaders for the current hash.
        if peers_caches_state
            .iter()
            .filter(|(_, data)| {
                (data.cache_hash != local_hash)
                    && (data.publication_timestamp
                        > (utils::get_epoch_ms() - (1000 * 2 * u128::from(SEND_CACHE_HASH_PERIOD))))
            })
            .count()
            > 0
        {
            if peers_caches_state
                .iter()
                .filter(|(peer_id, data)| {
                    (data.cache_hash == local_hash)
                        && (&**self.local_peer_id < peer_id.as_str())
                        && (data.publication_timestamp
                            > (utils::get_epoch_ms()
                                - (1000 * 2 * u128::from(SEND_CACHE_HASH_PERIOD))))
                })
                .count()
                > 0
            {
                // Our node is not the leader of the hash
                return (false, false);
            } else {
                // Our node is the leader of the hash
                return (false, true);
            }
        }

        // We are synchronized
        (true, true)
    }
}

impl<T> MessageHandler<T> {
    #[inline]
    fn to_mut(&mut self) -> MessageHandlerMut<'_, T> {
        let Self {
            cache,
            storage,
            is_persistent_cache,
            client_tx_channel,
            peers_caches_state,
            local_peer_id,
            publish_cache_counter,
            last_cache_repub_timestamp,
            swarm_sender,
        } = self;

        MessageHandlerMut {
            cache,
            storage,
            is_persistent_cache,
            client_tx_channel,
            peers_caches_state,
            local_peer_id,
            publish_cache_counter,
            last_cache_repub_timestamp,
            swarm_sender,
        }
    }
}

impl<T: DomoPersistentStorage> MessageHandler<T> {
    #[inline]
    async fn handle_message(&mut self, message: Message) {
        self.to_mut().handle_message(message).await;
    }

    #[inline]
    async fn send_cache_state(&mut self) {
        self.to_mut().send_cache_state().await
    }

    #[inline]
    async fn handle_internal_swarm_message(
        &mut self,
        swarm_message: InternalSwarmMessage,
    ) -> ControlFlow<Result<DomoEvent, Box<dyn std::error::Error>>> {
        self.to_mut()
            .handle_internal_swarm_message(swarm_message)
            .await
    }
}

fn write_cache(cache: &mut CacheTree, cache_element: DomoCacheElement) {
    // If topic_name is already present, insert into it, otherwise create a new map.
    // We could be using the entry api here together with or_default, but it would require copying
    // the key for the lookup, even if a reference would have been enough. We try to optimize more
    // for the reading use case, instead of the writing use case, so we rather try to avoid the
    // clone rather than the two map lookups. Once raw_entry APIs are available on stable Rust, we
    // can switch to those.

    if let Some(key) = cache.get_mut(&cache_element.topic_name) {
        key.insert(cache_element.topic_uuid.clone(), cache_element);
    } else {
        // first time that we add an element of topic_name type
        let mut map = BTreeMap::new();
        let topic_uuid = cache_element.topic_uuid.clone();
        let topic_name = cache_element.topic_name.clone();
        map.insert(topic_uuid, cache_element);
        cache.insert(topic_name, map);
    }
}

async fn insert_cache_element<T: DomoPersistentStorage>(
    cache: &mut CacheTree,
    storage: &mut T,
    swarm_sender: &SwarmSender,
    cache_element: DomoCacheElement,
    persist: bool,
    publish: bool,
) -> Option<DomoEvent> {
    write_cache(cache, cache_element.clone());

    if persist {
        storage.store(&cache_element);
    }

    if publish {
        gossip_pub(swarm_sender, cache_element, false).await
    } else {
        None
    }
}

#[inline]
async fn publish_gossipsub(
    swarm_sender: &SwarmSender,
    topic: libp2p::gossipsub::IdentTopic,
    data: Vec<u8>,
) -> swarm::Result<libp2p::gossipsub::MessageId, libp2p::gossipsub::error::PublishError> {
    swarm_sender
        .publish_gossipsub_with_return(topic, data)
        .await
}

async fn gossip_pub(
    swarm_sender: &SwarmSender,
    mut m: DomoCacheElement,
    republished: bool,
) -> Option<DomoEvent> {
    let topic = Topic::new("domo-persistent-data");

    if republished {
        m.republication_timestamp = utils::get_epoch_ms();
    } else {
        m.republication_timestamp = 0;
    }

    let m2 = serde_json::to_string(&m).unwrap();

    if let Err(e) = publish_gossipsub(swarm_sender, topic, m2.into()).await {
        log::info!("Publish error: {e:?}");
    }

    // signal a volatile pub by part of clients
    republished.not().then_some(DomoEvent::PersistentData(m))
}

async fn pub_message(
    swarm_sender: &SwarmSender,
    client_tx_channel: &UnboundedSender<DomoEvent>,
    value: Value,
) {
    let topic = Topic::new("domo-volatile-data");

    let m = serde_json::to_string(&value).unwrap();

    if let Err(e) = publish_gossipsub(swarm_sender, topic, m.into()).await {
        log::info!("Publish error: {:?}", e);
    }

    // signal a volatile pub by part of clients
    let ev = DomoEvent::VolatileData(value);
    client_tx_channel.send(ev).unwrap();
}

#[cfg(test)]
mod tests {
    use std::future::ready;

    use futures::StreamExt;
    use futures_concurrency::future::Race;

    use crate::domopersistentstorage::SqliteStorage;

    async fn make_cache() -> (
        super::DomoCacheSender,
        super::DomoCacheReceiver<SqliteStorage>,
    ) {
        let storage = SqliteStorage::new_in_memory();

        let shared_key =
            String::from("d061545647652562b4648f52e8373b3a417fc0df56c332154460da1801b341e9");

        let local_key = super::Keypair::generate_ed25519();

        super::channel(true, storage, shared_key, local_key, false, None).await
    }

    #[tokio::test]
    async fn test_delete() {
        let (domo_cache_sender, domo_cache_receiver) = make_cache().await;

        let checks = async move {
            domo_cache_sender
                .write_value(
                    "Domo::Light",
                    "luce-delete",
                    serde_json::json!({ "connected": true}),
                )
                .await;

            let _v = domo_cache_sender
                .read_cache_element("Domo::Light", "luce-delete")
                .await
                .unwrap();

            domo_cache_sender
                .delete_value("Domo::Light", "luce-delete")
                .await;

            let v = domo_cache_sender
                .read_cache_element("Domo::Light", "luce-delete")
                .await;

            assert_eq!(v, None);
        };

        (
            checks,
            domo_cache_receiver.into_stream().for_each(|_| ready(())),
        )
            .race()
            .await;
    }

    #[tokio::test]
    async fn test_write_and_read_key() {
        let (domo_cache_sender, domo_cache_receiver) = make_cache().await;

        let checks = async move {
            domo_cache_sender
                .write_value(
                    "Domo::Light",
                    "luce-1",
                    serde_json::json!({ "connected": true}),
                )
                .await;

            let val = domo_cache_sender
                .read_cache_element("Domo::Light", "luce-1")
                .await
                .unwrap()
                .value;
            assert_eq!(serde_json::json!({ "connected": true}), val);
        };

        (
            checks,
            domo_cache_receiver.into_stream().for_each(|_| ready(())),
        )
            .race()
            .await;
    }

    #[tokio::test]
    async fn test_write_twice_same_key() {
        let (domo_cache_sender, domo_cache_receiver) = make_cache().await;

        let checks = async move {
            domo_cache_sender
                .write_value(
                    "Domo::Light",
                    "luce-1",
                    serde_json::json!({ "connected": true}),
                )
                .await;

            let val = domo_cache_sender
                .read_cache_element("Domo::Light", "luce-1")
                .await
                .unwrap()
                .value;

            assert_eq!(serde_json::json!({ "connected": true}), val);

            domo_cache_sender
                .write_value(
                    "Domo::Light",
                    "luce-1",
                    serde_json::json!({ "connected": false}),
                )
                .await;

            let val = domo_cache_sender
                .read_cache_element("Domo::Light", "luce-1")
                .await
                .unwrap()
                .value;

            assert_eq!(serde_json::json!({ "connected": false}), val)
        };

        (
            checks,
            domo_cache_receiver.into_stream().for_each(|_| ready(())),
        )
            .race()
            .await;
    }

    #[tokio::test]
    async fn test_write_old_timestamp() {
        let (domo_cache_sender, domo_cache_receiver) = make_cache().await;

        let checks = async move {
            domo_cache_sender
                .write_value(
                    "Domo::Light",
                    "luce-timestamp",
                    serde_json::json!({ "connected": true}),
                )
                .await;

            let old_val = domo_cache_sender
                .read_cache_element("Domo::Light", "luce-timestamp")
                .await
                .unwrap();

            let el = super::DomoCacheElement {
                topic_name: String::from("Domo::Light"),
                topic_uuid: String::from("luce-timestamp"),
                value: Default::default(),
                deleted: false,
                publication_timestamp: 0,
                publisher_peer_id: domo_cache_sender.local_peer_id.to_string(),
                republication_timestamp: 0,
            };

            let ret = domo_cache_sender
                .write_with_timestamp_check("Domo::Light", "luce-timestamp", el)
                .await
                .unwrap();

            assert_eq!(ret, old_val);

            domo_cache_sender
                .write_value(
                    "Domo::Light",
                    "luce-timestamp",
                    serde_json::json!({ "connected": false}),
                )
                .await;

            let val = domo_cache_sender
                .read_cache_element("Domo::Light", "luce-timestamp")
                .await
                .unwrap();

            assert_ne!(ret, val);
        };

        (
            checks,
            domo_cache_receiver.into_stream().for_each(|_| ready(())),
        )
            .race()
            .await;
    }

    #[tokio::test]
    async fn test_filter_topic_name() {
        let (domo_cache_sender, domo_cache_receiver) = make_cache().await;

        let checks = async move {
            domo_cache_sender
                .write_value(
                    "Domo::Light",
                    "one",
                    serde_json::json!(
                        {
                             "description": "first_light",
                             "connected": false
                        }
                    ),
                )
                .await;

            domo_cache_sender
                .write_value(
                    "Domo::Light",
                    "two",
                    serde_json::json!(
                        {
                             "description": "second_light",
                             "connected": true
                        }
                    ),
                )
                .await;

            domo_cache_sender
                .write_value(
                    "Domo::Light",
                    "three",
                    serde_json::json!(
                        {
                             "description": "third_light",
                             "floor_number": 3
                        }
                    ),
                )
                .await;

            domo_cache_sender
                .write_value(
                    "Domo::Light",
                    "four",
                    serde_json::json!(
                        {
                             "description": "light_4",
                             "categories": [1, 2]
                        }
                    ),
                )
                .await;

            let mut filter_exp = "$.value[?(@.floor_number && @.floor_number > 2)].description";

            let values = domo_cache_sender
                .filter_with_topic_name("Domo::Light", filter_exp)
                .await
                .unwrap();

            let _str_value = values.to_string();

            assert_eq!(values, serde_json::json!(["third_light"]));

            filter_exp =
            "$.value[?(@.floor_number && @.floor_number > 2 && @.description ==\"third_light\")].description";

            let values = domo_cache_sender
                .filter_with_topic_name("Domo::Light", filter_exp)
                .await
                .unwrap();

            let _str_value = values.to_string();

            assert_eq!(values, serde_json::json!(["third_light"]));
        };

        (
            checks,
            domo_cache_receiver.into_stream().for_each(|_| ready(())),
        )
            .race()
            .await;
    }
}
