use thiserror::Error;

pub type Result<T> = std::result::Result<T, AegisError>;

#[derive(Debug, Error)]
pub enum AegisError {
    #[error("connection error: {0}")]
    Connection(String),

    #[error("registration error: {0}")]
    Registration(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("timeout waiting for daemon response")]
    Timeout,
}