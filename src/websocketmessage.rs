use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AsyncWebSocketDomoMessage {
    Volatile {
        value: serde_json::Value,
    },
    Persistent {
        value: serde_json::Value,
        topic_name: String,
        topic_uuid: String,
        deleted: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncWebSocketDomoRequest {
    RequestGetAll,
    RequestGetTopicName {
        topic_name: String,
    },
    RequestGetTopicUUID {
        topic_name: String,
        topic_uuid: String,
    },
    RequestDeleteTopicUUID {
        topic_name: String,
        topic_uuid: String,
    },
    RequestPubMessage {
        value: serde_json::Value,
    },
    RequestPostTopicUUID {
        topic_name: String,
        topic_uuid: String,
        value: serde_json::Value,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncWebSocketDomoRequestMessage {
    pub ws_client_id: String,
    pub req_id: String,
    pub request: SyncWebSocketDomoRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncWebSocketDomoResponseMessage {
    pub ws_client_id: String,
    pub req_id: String,
    pub response: String,
}
