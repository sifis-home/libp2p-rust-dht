use axum::{
    extract::Extension,
    http,
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};

use axum::extract::ws::Message;
use axum::extract::ws::WebSocketUpgrade;
use axum::extract::Path;

use tower_http::cors::{Any, CorsLayer};

use tokio::sync::{broadcast, mpsc, oneshot};

use tokio::sync::mpsc::Sender;

use serde_json::json;

use crate::websocketmessage::{
    AsyncWebSocketDomoMessage, SyncWebSocketDomoRequest, SyncWebSocketDomoRequestMessage,
    SyncWebSocketDomoResponseMessage,
};
use crate::{restmessage, utils};

use std::net::TcpListener;

#[derive(Debug, Clone)]
pub struct WebApiManagerSender {
    // channel where synchronous web socket responses are sent
    pub sync_tx_websocket_request: broadcast::Sender<SyncWebSocketDomoRequestMessage>,

    // channel where synchronous web socket responses are sent
    pub sync_tx_websocket_response: broadcast::Sender<SyncWebSocketDomoResponseMessage>,

    // channel where asynchronous web socket messages are sent
    pub async_tx_websocket: broadcast::Sender<AsyncWebSocketDomoMessage>,
}

pub struct WebApiManagerReceiver {
    // channel where synchronous web socket requests are sent
    pub sync_rx_websocket_request: broadcast::Receiver<SyncWebSocketDomoRequestMessage>,

    // channel where synchronous web socket requests are sent
    pub sync_rx_websocket_response: broadcast::Receiver<SyncWebSocketDomoResponseMessage>,

    // channel used to receive rest requests
    pub rx_rest: mpsc::Receiver<restmessage::RestMessage>,
}

pub fn create_web_api_manager(http_port: u16) -> (WebApiManagerSender, WebApiManagerReceiver) {
    //let addr = SocketAddr::from(([127, 0, 0, 1], http_port));

    let mut addr: String = "0.0.0.0:".to_owned();
    addr.push_str(&http_port.to_string());

    let listener = TcpListener::bind(addr).unwrap();

    #[cfg(unix)]
    {
        use nix::sys::socket::{self, sockopt::ReuseAddr, sockopt::ReusePort};
        use std::os::unix::io::AsRawFd;
        _ = socket::setsockopt(listener.as_raw_fd(), ReusePort, &true);

        _ = socket::setsockopt(listener.as_raw_fd(), ReuseAddr, &true);
    }

    let (tx_rest, rx_rest) = mpsc::channel(32);

    let tx_get_all = tx_rest.clone();

    let tx_get_topicname = tx_rest.clone();

    let tx_get_topicname_topicuuid = tx_rest.clone();

    let tx_post_topicname_topicuuid = tx_rest.clone();

    let tx_delete_topicname_topicuuid = tx_rest.clone();

    let tx_pub_message = tx_rest;

    let (async_tx_websocket, mut _async_rx_websocket) =
        broadcast::channel::<AsyncWebSocketDomoMessage>(16);

    let async_tx_websocket_copy = async_tx_websocket.clone();

    let (sync_tx_websocket_request, sync_rx_websocket_request) =
        broadcast::channel::<SyncWebSocketDomoRequestMessage>(16);
    let (sync_tx_websocket_response, sync_rx_websocket_response) =
        broadcast::channel::<SyncWebSocketDomoResponseMessage>(16);

    let sync_tx_websocket_copy = sync_tx_websocket_request.clone();

    let app = Router::new()
        // `GET /` goes to `root`
        .route(
            "/get_all",
            get(WebApiManagerSender::get_all_handler).layer(Extension(tx_get_all)),
        )
        .route(
            "/topic_name/:topic_name",
            get(WebApiManagerSender::get_topicname_handler).layer(Extension(tx_get_topicname)),
        )
        .route(
            "/topic_name/:topic_name/topic_uuid/:topic_uuid",
            get(WebApiManagerSender::get_topicname_topicuuid_handler)
                .layer(Extension(tx_get_topicname_topicuuid)),
        )
        .route(
            "/topic_name/:topic_name/topic_uuid/:topic_uuid",
            post(WebApiManagerSender::post_topicname_topicuuid_handler)
                .layer(Extension(tx_post_topicname_topicuuid)),
        )
        .route(
            "/topic_name/:topic_name/topic_uuid/:topic_uuid",
            delete(WebApiManagerSender::delete_topicname_topicuuid_handler)
                .layer(Extension(tx_delete_topicname_topicuuid)),
        )
        .route(
            "/pub",
            post(WebApiManagerSender::pub_message).layer(Extension(tx_pub_message)),
        )
        .route(
            "/ws",
            get(WebApiManagerSender::handle_websocket_req)
                .layer(Extension(async_tx_websocket_copy))
                .layer(Extension(sync_tx_websocket_copy))
                .layer(Extension(sync_tx_websocket_response.clone())),
        )
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers([http::header::CONTENT_TYPE]),
        );

    tokio::spawn(async move {
        axum::Server::from_tcp(listener)
            .unwrap()
            .serve(app.into_make_service())
            .await
    });

    let sender = WebApiManagerSender {
        sync_tx_websocket_request,
        sync_tx_websocket_response,
        async_tx_websocket,
    };

    let receiver = WebApiManagerReceiver {
        sync_rx_websocket_request,
        sync_rx_websocket_response,
        rx_rest,
    };

    (sender, receiver)
}

impl WebApiManagerSender {
    async fn delete_topicname_topicuuid_handler(
        Path((topic_name, topic_uuid)): Path<(String, String)>,
        Extension(tx_rest): Extension<Sender<restmessage::RestMessage>>,
    ) -> impl IntoResponse {
        let (tx_resp, rx_resp) = oneshot::channel();

        let m = restmessage::RestMessage::DeleteTopicUUID {
            topic_name,
            topic_uuid,
            responder: tx_resp,
        };

        tx_rest.send(m).await.unwrap();

        let resp = rx_resp.await.unwrap();

        match resp {
            Ok(resp) => (StatusCode::OK, Json(resp)),
            Err(_e) => (StatusCode::NOT_FOUND, Json(json!({}))),
        }
    }

    async fn pub_message(
        Json(value): Json<serde_json::Value>,
        Extension(tx_rest): Extension<Sender<restmessage::RestMessage>>,
    ) -> impl IntoResponse {
        let (tx_resp, rx_resp) = oneshot::channel();

        let m = restmessage::RestMessage::PubMessage {
            value,
            responder: tx_resp,
        };

        tx_rest.send(m).await.unwrap();

        let resp = rx_resp.await.unwrap();

        match resp {
            Ok(resp) => (StatusCode::OK, Json(resp)),
            Err(_e) => (StatusCode::NOT_FOUND, Json(json!({}))),
        }
    }

    async fn post_topicname_topicuuid_handler(
        Json(value): Json<serde_json::Value>,
        Path((topic_name, topic_uuid)): Path<(String, String)>,
        Extension(tx_rest): Extension<Sender<restmessage::RestMessage>>,
    ) -> impl IntoResponse {
        let (tx_resp, rx_resp) = oneshot::channel();

        let m = restmessage::RestMessage::PostTopicUUID {
            topic_name,
            topic_uuid,
            value,
            responder: tx_resp,
        };

        tx_rest.send(m).await.unwrap();

        let resp = rx_resp.await.unwrap();

        match resp {
            Ok(resp) => (StatusCode::OK, Json(resp)),
            Err(_e) => (StatusCode::NOT_FOUND, Json(json!({}))),
        }
    }

    async fn get_topicname_topicuuid_handler(
        Path((topic_name, topic_uuid)): Path<(String, String)>,
        Extension(tx_rest): Extension<Sender<restmessage::RestMessage>>,
    ) -> impl IntoResponse {
        let (tx_resp, rx_resp) = oneshot::channel();

        let m = restmessage::RestMessage::GetTopicUUID {
            topic_name,
            topic_uuid,
            responder: tx_resp,
        };

        tx_rest.send(m).await.unwrap();

        let resp = rx_resp.await.unwrap();

        match resp {
            Ok(resp) => (StatusCode::OK, Json(resp)),
            Err(_e) => (StatusCode::NOT_FOUND, Json(json!({}))),
        }
    }

    async fn get_topicname_handler(
        Path(topic_name): Path<String>,
        Extension(tx_rest): Extension<Sender<restmessage::RestMessage>>,
    ) -> impl IntoResponse {
        let (tx_resp, rx_resp) = oneshot::channel();

        let m = restmessage::RestMessage::GetTopicName {
            topic_name,
            responder: tx_resp,
        };
        tx_rest.send(m).await.unwrap();

        let resp = rx_resp.await.unwrap();

        match resp {
            Ok(resp) => (StatusCode::OK, Json(resp)),
            Err(_e) => (StatusCode::NOT_FOUND, Json(json!({}))),
        }
    }

    async fn get_all_handler(
        Extension(tx_rest): Extension<Sender<restmessage::RestMessage>>,
    ) -> impl IntoResponse {
        let (tx_resp, rx_resp) = oneshot::channel();

        let m = restmessage::RestMessage::GetAll { responder: tx_resp };

        tx_rest.send(m).await.unwrap();

        let resp = rx_resp.await.unwrap();

        (StatusCode::OK, Json(resp.unwrap()))
    }

    async fn handle_websocket_req(
        ws: WebSocketUpgrade,
        Extension(async_tx_ws): Extension<broadcast::Sender<AsyncWebSocketDomoMessage>>,
        Extension(sync_tx_ws_request): Extension<
            broadcast::Sender<SyncWebSocketDomoRequestMessage>,
        >,
        Extension(sync_tx_ws_response): Extension<
            broadcast::Sender<SyncWebSocketDomoResponseMessage>,
        >,
    ) -> impl IntoResponse {
        // channel for receiving async messages
        let mut async_rx_ws = async_tx_ws.subscribe();

        // channel for receiving sync messages responses
        let mut sync_rx_ws_response = sync_tx_ws_response.subscribe();

        ws.on_upgrade(|mut socket| async move {
            let my_id = utils::get_epoch_ms().to_string();

            loop {
                tokio::select! {
                        sync_rx = sync_rx_ws_response.recv() => {

                            let msg = sync_rx.unwrap();
                            if msg.ws_client_id == my_id {
                                let _ret = socket.send(Message::Text(msg.response)).await;
                            }
                        }
                        Some(msg) = socket.recv() => {

                            if let Err(_e) = msg {
                                return;
                            }

                            match msg.unwrap() {
                                Message::Text(message) => {
                                    // parso il messaggio
                                    println!("Received command {message}");

                                    let req : SyncWebSocketDomoRequest = serde_json::from_str(&message).unwrap();

                                    let msg = SyncWebSocketDomoRequestMessage {
                                        ws_client_id: my_id.clone(),
                                        req_id: my_id.clone(),
                                        request: req
                                    };

                                    let _ret = sync_tx_ws_request.send(msg);

                                }
                                Message::Close(_) => {
                                    return;
                                }
                                _ => {}
                            }
                        }
                        async_rx = async_rx_ws.recv() => {
                             let msg = async_rx.unwrap();
                             let string_msg = serde_json::to_string(&msg).unwrap();
                             let _ret = socket.send(Message::Text(string_msg)).await;
                        }
            }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use futures_concurrency::future::Join;

    #[tokio::test]
    async fn test_webapimanager_rest() {
        let (_webmanager_sender, mut webmanager_receiver) = super::create_web_api_manager(1234);

        let task = async {
            let _http_call = reqwest::get("http://localhost:1234/get_all").await;
        };

        let check = async {
            let ret = webmanager_receiver.rx_rest.recv().await;

            let message = ret.expect("success");
            let  crate::restmessage::RestMessage::GetAll { responder } = message else {
                panic!("unexpected rest message");
            };
            responder.send(Ok(serde_json::json!({}))).unwrap();
        };

        (task, check).join().await;
    }

    #[tokio::test]
    async fn test_webapimanager_websocket() {
        use futures_util::{SinkExt, StreamExt};

        use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

        let (_webmanager_sender, mut webmanager_receiver) = super::create_web_api_manager(1235);

        let task = async {
            let url = url::Url::parse("ws://localhost:1235/ws").unwrap();

            let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");

            let (mut write, _read) = ws_stream.split();

            write
                .send(Message::Text("\"RequestGetAll\"".to_owned()))
                .await
                .unwrap();
        };

        let check = async {
            let ret = webmanager_receiver.sync_rx_websocket_request.recv().await;

            let message = ret.expect("success");

            assert!(matches!(
                message.request,
                crate::websocketmessage::SyncWebSocketDomoRequest::RequestGetAll
            ));
        };

        (task, check).join().await;
    }
}
