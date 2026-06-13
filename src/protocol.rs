use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

pub const PROTOCOL_VERSION: &str = "0.1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MessageType {
    Control,
    Lifecycle,
    Config,
    Error,
    Heartbeat,
    Data,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Command {
    Register,
    Registered,
    StateUpdate,
    Shutdown,
    Reborn,
    Ack,
    Nack,
    Configure,
    Configured,
    Ping,
    Pong,
    RuntimeError,
    RegistrationFailed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ComponentState {
    Init,
    Registered,
    Initializing,
    Ready,
    Configured,
    Running,
    Waiting,
    Error,
    Finished,
    Shutdown,
}

impl std::fmt::Display for ComponentState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = serde_json::to_value(self)
            .ok()
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_default();
        write!(f, "{}", s)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub protocol_version: String,
    pub message_id:       String,
    pub correlation_id:   Option<String>,
    pub timestamp:        String,
    pub source:           String,
    pub target:           String,
    #[serde(rename = "type")]
    pub msg_type:         MessageType,
    pub command:          Command,
    pub payload:          HashMap<String, serde_json::Value>,
}

impl Envelope {
    pub fn new(
        msg_type: MessageType,
        command:  Command,
        source:   impl Into<String>,
        payload:  HashMap<String, serde_json::Value>,
    ) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION.to_string(),
            message_id:       Uuid::new_v4().to_string(),
            correlation_id:   None,
            timestamp:        Utc::now().to_rfc3339(),
            source:           source.into(),
            target:           "aegis".to_string(),
            msg_type,
            command,
            payload,
        }
    }

    pub fn with_correlation(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = Some(id.into());
        self
    }
}
