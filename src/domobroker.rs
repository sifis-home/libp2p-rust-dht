use std::future::ready;
use std::io::ErrorKind;

use crate::domocache::{self, DomoCacheReceiver, DomoCacheSender, DomoEvent, Message};
use crate::domopersistentstorage::SqliteStorage;
use crate::restmessage;
use crate::webapimanager::{create_web_api_manager, WebApiManagerReceiver, WebApiManagerSender};
use crate::websocketmessage::{
    AsyncWebSocketDomoMessage, SyncWebSocketDomoRequest, SyncWebSocketDomoRequestMessage,
    SyncWebSocketDomoResponseMessage,
};
use futures::{stream, FutureExt, Stream, StreamExt};
use futures_concurrency::future::Join;
use futures_concurrency::stream::Merge;
use libp2p::identity;
use rsa::pkcs8::EncodePrivateKey;
use rsa::RsaPrivateKey;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream};

pub struct DomoBrokerSender {
    domo_cache_sender: DomoCacheSender,
}

pub struct DomoBrokerReceiver {
    domo_cache_sender: DomoCacheSender,
    domo_cache_receiver: DomoCacheReceiver<SqliteStorage>,
    web_manager_receiver: WebApiManagerReceiver,
    web_manager_sender: WebApiManagerSender,
}

pub struct DomoBrokerConf {
    pub sqlite_file: String,
    pub private_key_file: Option<String>,
    pub is_persistent_cache: bool,
    pub shared_key: String,
    pub http_port: u16,
    pub loopback_only: bool,
    pub message_buffer_size: Option<usize>,
}

pub async fn create_domo_broker(
    conf: DomoBrokerConf,
) -> Result<(DomoBrokerSender, DomoBrokerReceiver), String> {
    if conf.sqlite_file.is_empty() {
        return Err(String::from("sqlite_file path needed"));
    }

    let storage = SqliteStorage::new(conf.sqlite_file, conf.is_persistent_cache);

    // Create a random local key.
    let mut pkcs8_der = if let Some(pk_path) = conf.private_key_file {
        match std::fs::read(&pk_path) {
            Ok(pem) => {
                let der = pem_rfc7468::decode_vec(&pem)
                    .map_err(|e| format!("Couldn't decode pem: {e:?}"))?;
                der.1
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {
                // Generate a new key and put it into the file at the given path
                let (pem, der) = generate_rsa_key();
                std::fs::write(pk_path, pem).expect("Couldn't save ");
                der
            }
            Err(e) => Err(format!("Couldn't load key file: {e:?}"))?,
        }
    } else {
        generate_rsa_key().1
    };
    let local_key = identity::Keypair::rsa_from_pkcs8(&mut pkcs8_der)
        .map_err(|e| format!("Couldn't load key: {e:?}"))?;

    let (domo_cache_sender, domo_cache_receiver) = domocache::channel(
        conf.is_persistent_cache,
        storage,
        conf.shared_key,
        local_key,
        conf.loopback_only,
        conf.message_buffer_size,
    )
    .await;

    let (web_manager_sender, web_manager_receiver) = create_web_api_manager(conf.http_port);

    let sender = DomoBrokerSender {
        domo_cache_sender: domo_cache_sender.clone(),
    };

    let receiver = DomoBrokerReceiver {
        web_manager_receiver,
        domo_cache_sender,
        domo_cache_receiver,
        web_manager_sender,
    };

    Ok((sender, receiver))
}

impl DomoBrokerSender {
    #[inline]
    pub fn local_peer_id(&self) -> &str {
        self.domo_cache_sender.local_peer_id()
    }

    #[inline]
    pub async fn print_peers_cache(&self) {
        self.domo_cache_sender.print_peers_cache().await;
    }

    #[inline]
    pub async fn print(&self) {
        self.domo_cache_sender.print().await;
    }

    #[inline]
    pub async fn print_cache_hash(&self) {
        self.domo_cache_sender.print_cache_hash().await;
    }

    #[inline]
    pub async fn write_value(&self, topic_name: &str, topic_uuid: &str, value: serde_json::Value) {
        self.domo_cache_sender
            .write_value(topic_name, topic_uuid, value)
            .await;
    }

    #[inline]
    pub async fn pub_value(&self, value: serde_json::Value) {
        self.domo_cache_sender.pub_value(value).await;
    }

    #[inline]
    pub async fn delete_value(&self, topic_name: &str, topic_uuid: &str) {
        self.domo_cache_sender
            .delete_value(topic_name, topic_uuid)
            .await;
    }
}

impl DomoBrokerReceiver {
    pub fn into_stream(self) -> impl Stream<Item = DomoEvent> + 'static {
        let Self {
            web_manager_receiver:
                WebApiManagerReceiver {
                    sync_rx_websocket_request,
                    rx_rest,
                    ..
                },
            domo_cache_sender,
            domo_cache_receiver,
            web_manager_sender:
                WebApiManagerSender {
                    sync_tx_websocket_response,
                    async_tx_websocket,
                    ..
                },
        } = self;

        let domo_cache_sender = domo_cache_sender.sender;

        let websocket_future = {
            let domo_cache_sender = domo_cache_sender.clone();
            BroadcastStream::new(sync_rx_websocket_request).for_each({
                move |webs_message| {
                    let message = webs_message.unwrap();
                    let domo_cache_sender = domo_cache_sender.clone();
                    let sync_tx_websocket_response = sync_tx_websocket_response.clone();
                    handle_websocket_sync_request(
                        domo_cache_sender,
                        sync_tx_websocket_response,
                        message,
                    )
                }
            })
        };

        let web_manager_future = ReceiverStream::new(rx_rest).for_each(move |rest_message| {
            handle_rest_request(domo_cache_sender.clone(), dbg!(rest_message))
        });

        (
            domo_cache_receiver.into_stream().map(move |message| {
                let out = message.map_or(DomoEvent::None, |message| {
                    handle_domo_event(async_tx_websocket.clone(), message)
                });
                Some(out)
            }),
            stream::once(
                (websocket_future, web_manager_future)
                    .join()
                    .map(|((), ())| None),
            ),
        )
            .merge()
            .filter_map(ready)
    }
}

async fn handle_websocket_sync_request(
    domo_cache_sender: mpsc::Sender<Message>,
    sync_tx_websocket_response: broadcast::Sender<SyncWebSocketDomoResponseMessage>,
    message: SyncWebSocketDomoRequestMessage,
) {
    let SyncWebSocketDomoRequestMessage {
        ws_client_id,
        req_id,
        request,
    } = message;
    let prepared_message = Message::websocket(ws_client_id, req_id, sync_tx_websocket_response);

    match request {
        SyncWebSocketDomoRequest::RequestGetAll => {
            println!("WebSocket RequestGetAll");

            prepared_message.get_all(&domo_cache_sender).await;
        }

        SyncWebSocketDomoRequest::RequestGetTopicName { topic_name } => {
            println!("WebSocket RequestGetTopicName");

            prepared_message
                .get_topic_name(topic_name, &domo_cache_sender)
                .await;
        }

        SyncWebSocketDomoRequest::RequestGetTopicUUID {
            topic_name,
            topic_uuid,
        } => {
            println!("WebSocket RequestGetTopicUUID");

            prepared_message
                .get_topic_uuid(topic_name, topic_uuid, &domo_cache_sender)
                .await;
        }

        SyncWebSocketDomoRequest::RequestDeleteTopicUUID {
            topic_name,
            topic_uuid,
        } => {
            println!("WebSocket RequestDeleteTopicUUID");

            prepared_message
                .delete_topic_uuid(topic_name, topic_uuid, &domo_cache_sender)
                .await;
        }

        SyncWebSocketDomoRequest::RequestPostTopicUUID {
            topic_name,
            topic_uuid,
            value,
        } => {
            println!("WebSocket RequestPostTopicUUID");

            prepared_message
                .post_topic_uuid(topic_name, topic_uuid, value, &domo_cache_sender)
                .await;
        }

        SyncWebSocketDomoRequest::RequestPubMessage { value } => {
            println!("WebSocket RequestPubMessage");
            prepared_message
                .pub_message(value, &domo_cache_sender)
                .await;
        }
    }
}

fn handle_domo_event(
    async_tx_websocket: broadcast::Sender<AsyncWebSocketDomoMessage>,
    m: DomoEvent,
) -> DomoEvent {
    match m {
        DomoEvent::PersistentData(m) => {
            println!(
                "Persistent message received {} {}",
                m.topic_name, m.topic_uuid
            );

            let m2 = m.clone();
            let _ret = async_tx_websocket.send(AsyncWebSocketDomoMessage::Persistent {
                topic_name: m.topic_name,
                topic_uuid: m.topic_uuid,
                value: m.value,
                deleted: m.deleted,
            });
            DomoEvent::PersistentData(m2)
        }
        DomoEvent::VolatileData(m) => {
            println!("Volatile message {m}");

            let m2 = m.clone();
            let _ret = async_tx_websocket.send(AsyncWebSocketDomoMessage::Volatile { value: m });

            DomoEvent::VolatileData(m2)
        }
        DomoEvent::None => DomoEvent::None,
    }
}

async fn handle_rest_request(
    domo_cache_sender: mpsc::Sender<Message>,
    rest_message: restmessage::RestMessage,
) {
    match rest_message {
        restmessage::RestMessage::GetAll { responder } => {
            Message::rest(responder).get_all(&domo_cache_sender).await;
        }
        restmessage::RestMessage::GetTopicName {
            topic_name,
            responder,
        } => {
            Message::rest(responder)
                .get_topic_name(topic_name, &domo_cache_sender)
                .await;
        }
        restmessage::RestMessage::GetTopicUUID {
            topic_name,
            topic_uuid,
            responder,
        } => {
            Message::rest(responder)
                .get_topic_uuid(topic_name, topic_uuid, &domo_cache_sender)
                .await;
        }
        restmessage::RestMessage::PostTopicUUID {
            topic_name,
            topic_uuid,
            value,
            responder,
        } => {
            Message::rest(responder)
                .post_topic_uuid(topic_name, topic_uuid, value, &domo_cache_sender)
                .await;
        }
        restmessage::RestMessage::DeleteTopicUUID {
            topic_name,
            topic_uuid,
            responder,
        } => {
            Message::rest(responder)
                .delete_topic_uuid(topic_name, topic_uuid, &domo_cache_sender)
                .await;
        }
        restmessage::RestMessage::PubMessage { value, responder } => {
            Message::rest(responder)
                .pub_message(value, &domo_cache_sender)
                .await;
        }
    }
}

fn generate_rsa_key() -> (Vec<u8>, Vec<u8>) {
    let mut rng = rand::thread_rng();
    let bits = 2048;
    let private_key = RsaPrivateKey::new(&mut rng, bits).expect("failed to generate a key");
    let pem = private_key
        .to_pkcs8_pem(Default::default())
        .unwrap()
        .as_bytes()
        .to_vec();
    let der = private_key.to_pkcs8_der().unwrap().as_ref().to_vec();
    (pem, der)
}

#[cfg(test)]
mod tests {
    use std::future::ready;

    use futures::{pin_mut, FutureExt, StreamExt};
    use futures_concurrency::future::{Join, Race};
    use serde_json::json;

    use crate::domobroker::DomoBrokerSender;
    use crate::domocache::PostTopicUuidResponse;
    use crate::websocketmessage::{AsyncWebSocketDomoMessage, SyncWebSocketDomoRequest};

    use super::DomoBrokerReceiver;

    async fn setup_broker(http_port: u16) -> (DomoBrokerSender, DomoBrokerReceiver) {
        let sqlite_file = crate::domopersistentstorage::SQLITE_MEMORY_STORAGE.to_owned();
        let domo_broker_conf = super::DomoBrokerConf {
            sqlite_file,
            is_persistent_cache: true,
            shared_key: String::from(
                "d061545647652562b4648f52e8373b3a417fc0df56c332154460da1801b341e9",
            ),
            private_key_file: None,
            http_port,
            loopback_only: false,
            message_buffer_size: None,
        };

        super::create_domo_broker(domo_broker_conf).await.unwrap()
    }

    #[tokio::test]
    async fn domo_broker_empty_cache() {
        let (_domo_broker_sender, domo_broker_receiver) = setup_broker(3000).await;

        let hnd = async move {
            let http_call = reqwest::get("http://localhost:3000/get_all").await.unwrap();

            let response: serde_json::Value = http_call.json().await.unwrap();
            assert_eq!(response, json!([]));
        };

        (
            domo_broker_receiver.into_stream().for_each(|_e| ready(())),
            hnd,
        )
            .race()
            .await;
    }

    #[tokio::test]
    async fn domo_broker_rest_get_all() {
        let (domo_broker_sender, domo_broker_receiver) = setup_broker(3001).await;

        let hnd = async move {
            domo_broker_sender
                .write_value("Domo::Light", "uno", serde_json::json!({"connected": true}))
                .await;

            domo_broker_sender
                .write_value("Domo::Light", "due", serde_json::json!({"connected": true}))
                .await;

            let http_call = reqwest::get("http://localhost:3001/get_all").await.unwrap();

            let response: serde_json::Value = http_call.json().await.unwrap();
            assert_eq!(
                response,
                serde_json::json!([
                    {
                        "topic_name": "Domo::Light",
                        "topic_uuid": "due",
                        "value": {
                            "connected": true
                        }
                    },
                    {
                        "topic_name": "Domo::Light",
                        "topic_uuid": "uno",
                        "value": {
                            "connected": true
                        }
                    }
                ])
            );
        };

        (
            domo_broker_receiver.into_stream().for_each(|_e| ready(())),
            hnd,
        )
            .race()
            .await;
    }

    #[tokio::test]
    async fn domo_broker_rest_get_topicname() {
        let (domo_broker_sender, domo_broker_receiver) = setup_broker(3002).await;

        let hnd = async move {
            domo_broker_sender
                .write_value("Domo::Light", "uno", serde_json::json!({"connected": true}))
                .await;

            domo_broker_sender
                .write_value(
                    "Domo::Socket",
                    "due",
                    serde_json::json!({"connected": true}),
                )
                .await;

            let http_call = reqwest::get("http://localhost:3002/topic_name/Domo::Light")
                .await
                .unwrap();

            let result: serde_json::Value = http_call.json().await.unwrap();
            assert_eq!(
                result,
                serde_json::json!([
                    {
                        "topic_name": "Domo::Light",
                        "topic_uuid": "uno",
                        "value": {
                            "connected": true
                        }
                    }
                ])
            );
        };

        (
            domo_broker_receiver.into_stream().for_each(|_e| ready(())),
            hnd,
        )
            .race()
            .await;
    }

    #[tokio::test]
    async fn domo_broker_rest_get_topicuuid() {
        let (domo_broker_sender, domo_broker_receiver) = setup_broker(3003).await;

        let hnd = async move {
            domo_broker_sender
                .write_value("Domo::Light", "uno", serde_json::json!({"connected": true}))
                .await;

            domo_broker_sender
                .write_value(
                    "Domo::Socket",
                    "due",
                    serde_json::json!({"connected": true}),
                )
                .await;

            let http_call =
                reqwest::get("http://localhost:3003/topic_name/Domo::Light/topic_uuid/uno")
                    .await
                    .unwrap();

            let result: serde_json::Value = http_call.json().await.unwrap();
            assert_eq!(
                result,
                serde_json::json!(
                    {
                        "topic_name": "Domo::Light",
                        "topic_uuid": "uno",
                        "value": {
                            "connected": true
                        }
                    }
                )
            );
        };

        (
            domo_broker_receiver.into_stream().for_each(|_e| ready(())),
            hnd,
        )
            .race()
            .await;
    }

    #[tokio::test]
    async fn domo_broker_rest_get_topicname_not_present() {
        let (domo_broker_sender, domo_broker_receiver) = setup_broker(3004).await;

        let hnd = async move {
            domo_broker_sender
                .write_value("Domo::Light", "uno", serde_json::json!({"connected": true}))
                .await;

            let http_call = reqwest::get("http://localhost:3004/topic_name/Domo::Not")
                .await
                .unwrap();

            let result: serde_json::Value = http_call.json().await.unwrap();
            assert_eq!(result, serde_json::json!([]));
        };

        (
            domo_broker_receiver.into_stream().for_each(|_e| ready(())),
            hnd,
        )
            .race()
            .await;
    }

    #[tokio::test]
    async fn domo_broker_rest_get_topicuuid_not_present() {
        let (domo_broker_sender, domo_broker_receiver) = setup_broker(3005).await;

        let hnd = async move {
            domo_broker_sender
                .write_value("Domo::Light", "uno", serde_json::json!({"connected": true}))
                .await;

            let http_call =
                reqwest::get("http://localhost:3005/topic_name/Domo::Light/topic_uuid/due")
                    .await
                    .unwrap();

            let result: serde_json::Value = http_call.json().await.unwrap();
            assert_eq!(result, serde_json::Value::Null);
        };

        (
            domo_broker_receiver.into_stream().for_each(|_e| ready(())),
            hnd,
        )
            .race()
            .await;
    }

    #[tokio::test]
    async fn domo_broker_rest_post_test() {
        use std::collections::HashMap;

        let (domo_broker_sender, domo_broker_receiver) = setup_broker(3006).await;

        let hnd = async move {
            let mut body = HashMap::new();
            body.insert("connected", true);

            let client = reqwest::Client::new();

            let _http_call = client
                .post("http://localhost:3006/topic_name/Domo::Light/topic_uuid/uno")
                .json(&body)
                .send()
                .await
                .unwrap();

            let ret = domo_broker_sender
                .domo_cache_sender
                .get_topic_uuid("Domo::Light", "uno")
                .await
                .unwrap();

            assert_eq!(
                ret,
                serde_json::json!(
                    {
                        "topic_name": "Domo::Light",
                        "topic_uuid": "uno",
                        "value": {
                            "connected": true
                        }
                    }
                )
            );
        };

        (
            domo_broker_receiver.into_stream().for_each(|_e| ready(())),
            hnd,
        )
            .race()
            .await;
    }

    #[tokio::test]
    async fn domo_broker_rest_delete_test() {
        let (domo_broker_sender, domo_broker_receiver) = setup_broker(3007).await;

        let hnd = async {
            domo_broker_sender
                .write_value("Domo::Light", "uno", serde_json::json!({"connected": true}))
                .await;

            let client = reqwest::Client::new();

            let ret = domo_broker_sender
                .domo_cache_sender
                .get_topic_uuid("Domo::Light", "uno")
                .await
                .unwrap();

            assert_eq!(
                ret,
                json!({
                    "topic_name": "Domo::Light",
                    "topic_uuid": "uno",
                    "value": {
                        "connected": true,
                    },
                }),
            );

            let _response = client
                .delete("http://localhost:3007/topic_name/Domo::Light/topic_uuid/uno")
                .send()
                .await
                .unwrap();

            let ret = domo_broker_sender
                .domo_cache_sender
                .get_topic_uuid("Domo::Light", "uno")
                .await
                .unwrap();

            assert_eq!(ret, serde_json::Value::Null);
        };

        (
            domo_broker_receiver.into_stream().for_each(|_e| ready(())),
            hnd,
        )
            .race()
            .await;
    }

    #[tokio::test]
    async fn domo_broker_rest_pub_test() {
        use crate::domocache::DomoEvent;
        use std::collections::HashMap;

        let (_domo_broker_sender, domo_broker_receiver) = setup_broker(3008).await;

        let hnd = async move {
            let mut body = HashMap::new();
            body.insert("message", "hello");

            let client = reqwest::Client::new();

            let _http_call = client
                .post("http://localhost:3008/pub")
                .json(&body)
                .send()
                .await
                .unwrap();
        };

        let domo_broker_receiver = domo_broker_receiver.into_stream();
        pin_mut!(domo_broker_receiver);
        (
            domo_broker_receiver
                .filter_map(|m| {
                    ready(match m {
                        DomoEvent::VolatileData(value) => Some(value),
                        _ => None,
                    })
                })
                .next()
                .map(|value| {
                    assert_eq!(value.unwrap(), serde_json::json!({"message": "hello"}));
                }),
            hnd,
        )
            .join()
            .await;
    }

    #[tokio::test]
    async fn domo_broker_test_websocket_empty() {
        use futures_util::{SinkExt, StreamExt};

        use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

        let (_domo_broker_sender, domo_broker_receiver) = setup_broker(3009).await;

        let check_future = async move {
            let url = url::Url::parse("ws://localhost:3009/ws").unwrap();

            let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");

            let (mut write, mut read) = ws_stream.split();

            write
                .send(Message::Text("\"RequestGetAll\"".to_owned()))
                .await
                .unwrap();

            let msg = read.next().await.unwrap().unwrap();

            let Message::Text(text) = msg else {
                panic!("unexpected message");
            };

            assert_eq!(text.parse::<serde_json::Value>().unwrap(), json!([]));
        };

        (
            domo_broker_receiver.into_stream().for_each(|_e| ready(())),
            check_future,
        )
            .race()
            .await;
    }

    #[tokio::test]
    async fn domo_broker_test_websocket_getall() {
        use futures_util::{SinkExt, StreamExt};

        use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

        let (domo_broker_sender, domo_broker_receiver) = setup_broker(3010).await;

        let check_future = async move {
            domo_broker_sender
                .write_value("Domo::Light", "uno", serde_json::json!({"connected": true}))
                .await;

            domo_broker_sender
                .write_value("Domo::Light", "due", serde_json::json!({"connected": true}))
                .await;

            let url = url::Url::parse("ws://localhost:3010/ws").unwrap();

            let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");

            let (mut write, mut read) = ws_stream.split();

            write
                .send(Message::Text("\"RequestGetAll\"".to_owned()))
                .await
                .unwrap();

            let msg = read.next().await.unwrap().unwrap();
            let Message::Text(text) = msg else {
                panic!("unexpected message");
            };

            assert_eq!(
                text.parse::<serde_json::Value>().unwrap(),
                json!([
                    {
                        "topic_name": "Domo::Light",
                        "topic_uuid": "due",
                        "value": {
                            "connected": true
                        }
                    },
                    {
                        "topic_name": "Domo::Light",
                        "topic_uuid": "uno",
                        "value": {
                            "connected": true
                        }
                    }
                ])
            );
        };

        (
            domo_broker_receiver.into_stream().for_each(|_e| ready(())),
            check_future,
        )
            .race()
            .await;
    }

    #[tokio::test]
    async fn domo_broker_test_websocket_get_topicname() {
        use futures_util::{SinkExt, StreamExt};

        use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

        let (domo_broker_sender, domo_broker_receiver) = setup_broker(3011).await;

        let check_future = async move {
            domo_broker_sender
                .write_value("Domo::Light", "uno", serde_json::json!({"connected": true}))
                .await;

            domo_broker_sender
                .write_value(
                    "Domo::Socket",
                    "due",
                    serde_json::json!({"connected": true}),
                )
                .await;

            let url = url::Url::parse("ws://localhost:3011/ws").unwrap();

            let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");

            let (mut write, mut read) = ws_stream.split();

            let req = serde_json::to_string(&SyncWebSocketDomoRequest::RequestGetTopicName {
                topic_name: "Domo::Light".to_string(),
            })
            .unwrap();

            write.send(Message::Text(req)).await.unwrap();

            let msg = read.next().await.unwrap().unwrap();
            let Message::Text(text) = msg else {
                panic!("unexpected message");
            };
            let msg: serde_json::Value = serde_json::from_str(&text).unwrap();

            assert_eq!(
                msg,
                json!([
                    {
                        "topic_name": "Domo::Light",
                        "topic_uuid": "uno",
                        "value": {
                            "connected": true
                        }
                    }
                ])
            );
        };

        (
            domo_broker_receiver.into_stream().for_each(|_e| ready(())),
            check_future,
        )
            .race()
            .await;
    }

    #[tokio::test]
    async fn domo_broker_test_websocket_get_topicuuid() {
        use futures_util::{SinkExt, StreamExt};

        use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

        let (domo_broker_sender, domo_broker_receiver) = setup_broker(3012).await;

        let check_future = async move {
            domo_broker_sender
                .write_value("Domo::Light", "uno", serde_json::json!({"connected": true}))
                .await;

            domo_broker_sender
                .write_value(
                    "Domo::Socket",
                    "due",
                    serde_json::json!({"connected": true}),
                )
                .await;

            let url = url::Url::parse("ws://localhost:3012/ws").unwrap();

            let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");

            let (mut write, mut read) = ws_stream.split();

            let req = serde_json::to_string(&SyncWebSocketDomoRequest::RequestGetTopicUUID {
                topic_name: "Domo::Light".to_string(),
                topic_uuid: "uno".to_string(),
            })
            .unwrap();

            write.send(Message::Text(req)).await.unwrap();

            let msg = read.next().await.unwrap().unwrap();
            let Message::Text(text) = msg else {
                panic!("unexpected message");
            };
            let msg: serde_json::Value = serde_json::from_str(&text).unwrap();

            assert_eq!(
                msg,
                json!(
                    {
                        "topic_name": "Domo::Light",
                        "topic_uuid": "uno",
                        "value": {
                            "connected": true
                        }
                    }
                )
            );
        };

        (
            domo_broker_receiver.into_stream().for_each(|_e| ready(())),
            check_future,
        )
            .race()
            .await;
    }

    #[tokio::test]
    async fn domo_broker_test_websocket_post() {
        use futures_util::{SinkExt, StreamExt};

        use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

        let (_domo_broker_sender, domo_broker_receiver) = setup_broker(3013).await;

        let check_future = async move {
            let url = url::Url::parse("ws://localhost:3013/ws").unwrap();

            let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");

            let (mut write, mut read) = ws_stream.split();

            let req = serde_json::to_string(&SyncWebSocketDomoRequest::RequestPostTopicUUID {
                topic_name: "Domo::Light".to_string(),
                topic_uuid: "uno".to_string(),
                value: serde_json::json!({"connected": true}),
            })
            .unwrap();

            write.send(Message::Text(req)).await.unwrap();

            let msg = read.next().await.unwrap().unwrap();

            let Message::Text(text) = msg else {
                panic!("unexpected message");
            };
            let msg: Option<PostTopicUuidResponse> = serde_json::from_str(&text).unwrap();

            assert_eq!(
                msg.unwrap(),
                PostTopicUuidResponse::Persistent {
                    topic_name: "Domo::Light".to_owned(),
                    topic_uuid: "uno".to_owned(),
                    value: serde_json::json!({"connected": true}),
                    deleted: false,
                }
            );
        };

        (
            domo_broker_receiver.into_stream().for_each(|_e| ready(())),
            check_future,
        )
            .race()
            .await;
    }

    #[tokio::test]
    async fn domo_broker_test_websocket_delete() {
        use futures_util::{SinkExt, StreamExt};

        use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

        let (domo_broker_sender, domo_broker_receiver) = setup_broker(3014).await;

        let check_future = async move {
            domo_broker_sender
                .write_value("Domo::Light", "uno", serde_json::json!({"connected": true}))
                .await;

            let url = url::Url::parse("ws://localhost:3014/ws").unwrap();

            let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");

            let (mut write, mut read) = ws_stream.split();

            let req = serde_json::to_string(&SyncWebSocketDomoRequest::RequestDeleteTopicUUID {
                topic_name: "Domo::Light".to_string(),
                topic_uuid: "uno".to_string(),
            })
            .unwrap();

            write.send(Message::Text(req)).await.unwrap();

            let msg = read.next().await.unwrap().unwrap();

            let Message::Text(text) = msg else {
                panic!("unexpected message");
            };

            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&text).unwrap(),
                json!({
                    "topic_name": "Domo::Light",
                    "topic_uuid": "uno",
                    "value": null,
                    "deleted": true,
                })
            );
        };

        (
            domo_broker_receiver.into_stream().for_each(|_e| ready(())),
            check_future,
        )
            .race()
            .await;
    }

    #[tokio::test]
    async fn domo_broker_test_websocket_pub() {
        use futures_util::{SinkExt, StreamExt};

        use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

        let (_domo_broker_sender, domo_broker_receiver) = setup_broker(3015).await;

        let check_future = async move {
            let url = url::Url::parse("ws://localhost:3015/ws").unwrap();

            let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");

            let (mut write, mut read) = ws_stream.split();

            let req = serde_json::to_string(&SyncWebSocketDomoRequest::RequestPubMessage {
                value: serde_json::json!({"message": "hello"}),
            })
            .unwrap();

            write.send(Message::Text(req)).await.unwrap();

            let msg = read.next().await.unwrap().unwrap();

            let Message::Text(text) = msg else {
                panic!("unexpected message");
            };

            assert_eq!(
                serde_json::from_str::<AsyncWebSocketDomoMessage>(&text).unwrap(),
                AsyncWebSocketDomoMessage::Volatile {
                    value: serde_json::json!({"message": "hello"}),
                }
            );
        };

        (
            domo_broker_receiver.into_stream().for_each(|_e| ready(())),
            check_future,
        )
            .race()
            .await;
    }
}
