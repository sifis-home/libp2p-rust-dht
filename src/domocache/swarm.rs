use core::fmt;
use std::{future::ready, result};

use futures::{Stream, StreamExt};
use futures_concurrency::stream::Merge;
use libp2p::{gossipsub, swarm::SwarmEvent};
use time::OffsetDateTime;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;

use crate::domolibp2p::DomoBehaviour;

use super::DomoSwarmEvent;

pub fn channel(swarm: libp2p::Swarm<DomoBehaviour>, buffer: usize) -> (SwarmSender, SwarmReceiver) {
    let (sender, receiver) = mpsc::channel(buffer);
    let sender = SwarmSender(sender);
    let receiver = SwarmReceiver {
        inner: swarm,
        receiver,
    };
    (sender, receiver)
}

pub struct SwarmReceiver {
    inner: libp2p::Swarm<DomoBehaviour>,
    receiver: mpsc::Receiver<SwarmOperation>,
}

#[derive(Debug)]
pub enum SwarmOperation {
    PublishGossipsubWithReturn {
        topic: gossipsub::IdentTopic,
        data: Vec<u8>,
        sender: oneshot::Sender<Result<gossipsub::MessageId, gossipsub::error::PublishError>>,
    },
}

#[derive(Debug)]
pub struct SwarmSender(pub mpsc::Sender<SwarmOperation>);

#[derive(Debug)]
pub enum ChannelSendError {
    SwarmChannel,
    ResultChannel,
}

impl<T> From<mpsc::error::SendError<T>> for ChannelSendError {
    #[inline]
    fn from(_value: mpsc::error::SendError<T>) -> Self {
        ChannelSendError::SwarmChannel
    }
}

impl From<oneshot::error::RecvError> for ChannelSendError {
    #[inline]
    fn from(_value: oneshot::error::RecvError) -> Self {
        ChannelSendError::ResultChannel
    }
}

impl fmt::Display for ChannelSendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChannelSendError::SwarmChannel => f.write_str("swarm send channel is closed"),
            ChannelSendError::ResultChannel => {
                f.write_str("oneshot result channel for swarm response is closed")
            }
        }
    }
}

impl std::error::Error for ChannelSendError {}

#[derive(Debug)]
pub enum SendError<E> {
    Channel(ChannelSendError),
    Swarm(E),
}

impl<T, E> From<mpsc::error::SendError<T>> for SendError<E> {
    #[inline]
    fn from(value: mpsc::error::SendError<T>) -> Self {
        SendError::Channel(value.into())
    }
}

impl<E> From<oneshot::error::RecvError> for SendError<E> {
    #[inline]
    fn from(value: oneshot::error::RecvError) -> Self {
        SendError::Channel(value.into())
    }
}

impl<E: fmt::Display> fmt::Display for SendError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::Channel(err) => write!(f, "{}", err),
            SendError::Swarm(err) => write!(f, "{}", err),
        }
    }
}

impl<E: std::error::Error> std::error::Error for SendError<E> {}

pub type Result<T, E> = result::Result<T, SendError<E>>;

impl SwarmSender {
    pub async fn publish_gossipsub_with_return(
        &self,
        topic: gossipsub::IdentTopic,
        data: Vec<u8>,
    ) -> Result<gossipsub::MessageId, gossipsub::error::PublishError> {
        let (sender, receiver) = oneshot::channel();
        let message = SwarmOperation::PublishGossipsubWithReturn {
            topic,
            data,
            sender,
        };
        self.0.send(message).await?;
        receiver.await?
    }
}

impl SwarmReceiver {
    pub fn into_stream(self) -> impl Stream<Item = DomoSwarmEvent> {
        // TODO: check if it's worth boxing
        #[allow(clippy::large_enum_variant)]
        enum Message {
            Swarm(DomoSwarmEvent),
            Op(SwarmOperation),
        }

        let Self {
            inner: swarm,
            receiver,
        } = self;

        // This case is analogous to the one explained in `DomoCacheReceiver::into_stream`: the
        // _sync_ mutex inside `gossipsub` should be never locked (which would just panic in our
        // case) because the polling of swarm and the handling of obtained messages are
        // intrinsically mutually exclusive. In fact, this behaviour could be better implemented
        // using a _lending stream_, which however would be insufficient to avoid the `Arc<Mutex>`
        // because of the libp2p architecture.
        //
        // For any doubt, please try to prove me wrong about these assumption: if you are able to
        // make this code panic, there is surely something wrong.
        let gossipsub = swarm.behaviour().gossipsub.clone();

        (
            swarm.map(Message::Swarm),
            ReceiverStream::new(receiver).map(Message::Op),
        )
            .merge()
            .filter_map(move |message| {
                let out = match message {
                    Message::Swarm(SwarmEvent::Behaviour(crate::domolibp2p::OutEvent::Mdns(
                        libp2p::mdns::Event::Discovered(list),
                    ))) => {
                        let date = OffsetDateTime::now_utc();
                        let mut gossipsub = gossipsub.0.try_lock().unwrap();
                        for (peer, _) in list {
                            gossipsub.add_explicit_peer(&peer);
                            log::info!("Discovered peer {peer} {date:?}");
                        }
                        None
                    }

                    Message::Swarm(event) => Some(event),

                    Message::Op(SwarmOperation::PublishGossipsubWithReturn {
                        topic,
                        data,
                        sender,
                    }) => {
                        let result = gossipsub.0.try_lock().unwrap().publish(topic, data);
                        // TODO: should this error bubble up?
                        sender.send(result.map_err(SendError::Swarm)).unwrap();
                        None
                    }
                };

                ready(out)
            })
    }
}
