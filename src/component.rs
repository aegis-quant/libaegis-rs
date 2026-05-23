use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use serde_json::Value;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    sync::Mutex,
    time::sleep,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::{
    config::Config,
    error::{AegisError, Result},
    protocol::{Command, ComponentState, Envelope, MessageType},
};

// ---------------------------------------------------------------------------
// ComponentHandler trait
// ---------------------------------------------------------------------------

#[allow(unused_variables)]
pub trait ComponentHandler: Send + Sync + 'static {
    /// Called when Aegis sends a CONFIGURE message.
    /// `socket_path` and `topics` come directly from the CONFIGURE payload.
    fn on_configure(
        &self,
        socket_path: String,
        topics: Vec<String>,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    /// Called after the component transitions to RUNNING.
    ///
    /// Start your main processing loop here. The `shutdown` token is
    /// cancelled only on SHUTDOWN. On a session restart Aegis sends REBORN
    /// instead — `on_running` keeps going without interruption. Reset
    /// per-run state in `on_reborn`.
    fn on_running(
        &self,
        component_id: String,
        session_id:   String,
        shutdown:     CancellationToken,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    /// Called when Aegis sends REBORN — the session is restarting.
    ///
    /// Reset ALL internal state from the previous run (positions, POCs,
    /// buffers, counters…). After this returns the SDK sends ACK and the
    /// component stays RUNNING — no reconnect, no new CONFIGURE.
    fn on_reborn(&self) -> impl std::future::Future<Output = ()> + Send {
        async {}
    }

    /// Called on every PING before the PONG is sent.
    fn on_ping(&self) -> impl std::future::Future<Output = ()> + Send {
        async {}
    }

    /// Called just before the component disconnects.
    fn on_shutdown(&self) -> impl std::future::Future<Output = ()> + Send {
        async {}
    }

    /// Called when Aegis sends a non-recoverable ERROR.
    fn on_error(
        &self,
        code: String,
        message: String,
    ) -> impl std::future::Future<Output = ()> + Send {
        async {}
    }
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

pub struct Component<H: ComponentHandler> {
    cfg:     Config,
    handler: Arc<H>,

    pub component_id: Arc<Mutex<String>>,
    pub session_id:   Arc<Mutex<String>>,
    pub state:        Arc<Mutex<ComponentState>>,

    started_at: Instant,
}

impl<H: ComponentHandler> Component<H> {
    pub fn new(cfg: Config, handler: H) -> Self {
        Self {
            cfg,
            handler:      Arc::new(handler),
            component_id: Arc::new(Mutex::new(String::new())),
            session_id:   Arc::new(Mutex::new(String::new())),
            state:        Arc::new(Mutex::new(ComponentState::Init)),
            started_at:   Instant::now(),
        }
    }

    pub async fn run(&self) -> Result<()> {
        let mut attempts: u32 = 0;
        let mut delay = self.cfg.reconnect_delay;

        loop {
            match self.run_once().await {
                Ok(()) => return Ok(()),

                Err(AegisError::Registration(msg)) => {
                    error!("Registration failed (will not retry): {}", msg);
                    return Err(AegisError::Registration(msg));
                }

                Err(e) => warn!("Disconnected: {}", e),
            }

            if !self.cfg.reconnect {
                return Err(AegisError::Connection("reconnect disabled".into()));
            }

            attempts += 1;
            if self.cfg.max_reconnect_attempts > 0
                && attempts >= self.cfg.max_reconnect_attempts
            {
                return Err(AegisError::Connection(format!(
                    "max reconnect attempts ({}) reached",
                    attempts
                )));
            }

            info!("Reconnecting in {:?} (attempt {})...", delay, attempts);
            sleep(delay).await;
            delay = delay.mul_f32(2.0).min(self.cfg.max_reconnect_delay);
        }
    }

    pub async fn send_state_update(
        &self,
        writer: &Arc<Mutex<tokio::net::unix::OwnedWriteHalf>>,
        new_state: ComponentState,
        message: Option<&str>,
    ) -> Result<()> {
        let mut payload = HashMap::new();
        payload.insert("state".into(), Value::String(format!("{}", new_state)));
        if let Some(msg) = message {
            payload.insert("message".into(), Value::String(msg.to_string()));
        }
        let env = Envelope::new(
            MessageType::Lifecycle,
            Command::StateUpdate,
            self.source(),
            payload,
        );
        self.send_envelope(writer, &env).await?;
        *self.state.lock().await = new_state;
        Ok(())
    }

    pub async fn send_error(
        &self,
        writer: &Arc<Mutex<tokio::net::unix::OwnedWriteHalf>>,
        code: &str,
        message: &str,
        recoverable: bool,
    ) -> Result<()> {
        let mut payload = HashMap::new();
        payload.insert("code".into(),        Value::String(code.to_string()));
        payload.insert("message".into(),     Value::String(message.to_string()));
        payload.insert("recoverable".into(), Value::Bool(recoverable));
        let env = Envelope::new(
            MessageType::Error,
            Command::RuntimeError,
            self.source(),
            payload,
        );
        self.send_envelope(writer, &env).await
    }

    // ------------------------------------------------------------------
    // Internal
    // ------------------------------------------------------------------

    async fn run_once(&self) -> Result<()> {
        info!("Connecting to {}", self.cfg.socket_path);
        let stream = UnixStream::connect(&self.cfg.socket_path).await?;
        let (read_half, write_half) = stream.into_split();

        let writer = Arc::new(Mutex::new(write_half));
        let mut reader = BufReader::new(read_half);

        info!("Connected");
        self.register(&writer, &mut reader).await?;
        self.message_loop(&writer, &mut reader).await
    }

    async fn register(
        &self,
        writer: &Arc<Mutex<tokio::net::unix::OwnedWriteHalf>>,
        reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
    ) -> Result<()> {
        // Build REGISTER payload.
        // Prefer the value set in Config; fall back to AEGIS_SESSION_TOKEN env
        // var injected by the Aegis daemon — mirrors the Go SDK behaviour.
        let session_token = if !self.cfg.session_token.is_empty() {
            self.cfg.session_token.clone()
        } else {
            std::env::var("AEGIS_SESSION_TOKEN").unwrap_or_default()
        };

        let mut payload = HashMap::new();
        payload.insert("session_token".into(), Value::String(session_token));
        payload.insert("component_name".into(), Value::String(self.cfg.component_name.clone()));
        payload.insert("version".into(),        Value::String(self.cfg.version.clone()));

        // Prefer the internally stored ID (set after first REGISTERED response).
        // On the very first connect it will be empty — fall back to the env var
        // that LaunchComponents injects so Aegis can match the pre-assigned
        // placeholder in the registry (Case A in manager.go).
        let stored_id = self.component_id.lock().await.clone();
        let component_id = if !stored_id.is_empty() {
            stored_id
        } else {
            std::env::var("AEGIS_COMPONENT_ID").unwrap_or_default()
        };
        if !component_id.is_empty() {
            payload.insert("component_id".into(), Value::String(component_id));
        }

        payload.insert("capabilities".into(), serde_json::json!({
            "supported_symbols":          self.cfg.supported_symbols,
            "supported_timeframes":       self.cfg.supported_timeframes,
            "supported_orderbook_speeds": self.cfg.supported_orderbook_speeds,
            "requires_streams":           self.cfg.requires_streams,
        }));

        let env = Envelope::new(
            MessageType::Lifecycle,
            Command::Register,
            self.source(),
            payload,
        );
        self.send_envelope(writer, &env).await?;

        // Wait for REGISTERED
        let resp = self.recv_envelope(reader).await?;
        if resp.command != Command::Registered {
            return Err(AegisError::Registration(format!(
                "unexpected response: {:?}",
                resp.command
            )));
        }

        *self.component_id.lock().await = resp.payload["component_id"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        *self.session_id.lock().await = resp.payload["session_id"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        *self.state.lock().await = ComponentState::Registered;

        info!(
            "Registered — component_id={} session_id={}",
            self.component_id.lock().await,
            self.session_id.lock().await,
        );

        // Aegis (WaitForReady) expects STATE_UPDATE(INITIALIZING) then
        // STATE_UPDATE(READY) before it sends CONFIGURE.
        self.send_state_update(writer, ComponentState::Initializing, None).await?;
        self.send_state_update(writer, ComponentState::Ready, None).await?;

        Ok(())
    }

    async fn message_loop(
        &self,
        writer: &Arc<Mutex<tokio::net::unix::OwnedWriteHalf>>,
        reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
    ) -> Result<()> {
        // One token per connection. Cancelled only on SHUTDOWN.
        // REBORN does NOT cancel it — on_running keeps processing across restarts.
        let shutdown_token = CancellationToken::new();

        loop {
            let env = tokio::time::timeout(
                Duration::from_secs(35),
                self.recv_envelope(reader),
            )
            .await
            .map_err(|_| AegisError::Timeout)??;

            debug!("Received type={:?} command={:?}", env.msg_type, env.command);

            match env.msg_type {
                MessageType::Heartbeat => {
                    self.handle_heartbeat(writer, &env).await?;
                }
                MessageType::Config => {
                    self.handle_config(writer, &env, shutdown_token.clone()).await?;
                }
                MessageType::Lifecycle => {
                    match self.handle_lifecycle(writer, &env, &shutdown_token).await? {
                        LifecycleOutcome::Continue => {}
                        LifecycleOutcome::Stop => return Ok(()),
                    }
                }
                MessageType::Error => {
                    if self.handle_error_msg(&env).await {
                        return Err(AegisError::Connection("non-recoverable error".into()));
                    }
                }
                // ACKs sent by Aegis in response to STATE_UPDATEs arrive as
                // Control/Ack — silently ignore them.
                MessageType::Control => {
                    debug!("Control/{:?} — ignored", env.command);
                }
                _ => warn!("Unknown message type: {:?}", env.msg_type),
            }
        }
    }

    async fn handle_heartbeat(
        &self,
        writer: &Arc<Mutex<tokio::net::unix::OwnedWriteHalf>>,
        env: &Envelope,
    ) -> Result<()> {
        if env.command != Command::Ping {
            return Ok(());
        }
        self.handler.on_ping().await;

        let uptime = self.started_at.elapsed().as_secs();
        let mut payload = HashMap::new();
        payload.insert("state".into(),          Value::String(format!("{}", *self.state.lock().await)));
        payload.insert("uptime_seconds".into(), Value::Number(uptime.into()));

        let pong = Envelope::new(
            MessageType::Heartbeat,
            Command::Pong,
            self.source(),
            payload,
        )
        .with_correlation(env.message_id.clone());

        self.send_envelope(writer, &pong).await?;
        debug!("Sent PONG (uptime={}s)", uptime);
        Ok(())
    }

    async fn handle_config(
        &self,
        writer: &Arc<Mutex<tokio::net::unix::OwnedWriteHalf>>,
        env: &Envelope,
        shutdown_token: CancellationToken,
    ) -> Result<()> {
        if env.command != Command::Configure {
            return Ok(());
        }

        let socket_path = env.payload.get("data_stream_socket")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        let topics: Vec<String> = env.payload.get("topics")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
            .unwrap_or_default();

        info!("Configuring — socket={} topics={:?}", socket_path, topics);

        if let Err(e) = self.handler.on_configure(socket_path, topics).await {
            error!("on_configure error: {}", e);
            self.send_error(writer, "CONFIGURE_FAILED", &e.to_string(), true).await?;
            return Ok(());
        }

        // 1. ACK the CONFIGURE message
        self.send_ack(writer, &env.message_id).await?;

        // 2. Transition to CONFIGURED — Aegis ACKs via handleLifecycleMessage
        self.send_state_update(writer, ComponentState::Configured, None).await?;

        // 3. Transition to RUNNING
        self.send_state_update(writer, ComponentState::Running, None).await?;

        info!("Component is now RUNNING");

        let handler      = Arc::clone(&self.handler);
        let component_id = self.component_id.lock().await.clone();
        let session_id   = self.session_id.lock().await.clone();
        tokio::spawn(async move {
            if let Err(e) = handler.on_running(component_id, session_id, shutdown_token).await {
                error!("on_running error: {}", e);
            }
        });

        Ok(())
    }

    async fn handle_lifecycle(
        &self,
        writer: &Arc<Mutex<tokio::net::unix::OwnedWriteHalf>>,
        env: &Envelope,
        shutdown_token: &CancellationToken,
    ) -> Result<LifecycleOutcome> {
        match env.command {
            Command::Reborn => {
                info!("REBORN — clearing state for session restart");

                // on_running keeps running — it will receive fresh data from
                // the orchestrator on the existing stream socket.
                self.handler.on_reborn().await;
                self.send_ack(writer, &env.message_id).await?;

                info!("REBORN complete — continuing");
                Ok(LifecycleOutcome::Continue)
            }

            Command::Shutdown => {
                info!("Shutdown requested by Aegis");
                shutdown_token.cancel();
                self.send_ack(writer, &env.message_id).await?;
                self.graceful_shutdown().await;
                Ok(LifecycleOutcome::Stop)
            }

            Command::Ack => {
                debug!("ACK received");
                Ok(LifecycleOutcome::Continue)
            }

            _ => {
                warn!("Unknown lifecycle command: {:?}", env.command);
                Ok(LifecycleOutcome::Continue)
            }
        }
    }

    async fn handle_error_msg(&self, env: &Envelope) -> bool {
        let code = env.payload.get("code")
            .and_then(|v| v.as_str())
            .unwrap_or("UNKNOWN");
        let message = env.payload.get("message")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let recoverable = env.payload.get("recoverable")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        error!("Error from Aegis — code={} message={}", code, message);
        self.handler.on_error(code.to_string(), message.to_string()).await;
        !recoverable // true = fatal, triggers reconnect
    }

    async fn graceful_shutdown(&self) {
        info!("Shutting down...");
        self.handler.on_shutdown().await;
        *self.state.lock().await = ComponentState::Shutdown;
        info!("Shutdown complete");
        // Connection closes naturally when run_once returns, giving the
        // server a clean EOF instead of an abrupt disconnect.
    }

    async fn send_ack(
        &self,
        writer: &Arc<Mutex<tokio::net::unix::OwnedWriteHalf>>,
        correlation_id: &str,
    ) -> Result<()> {
        let mut payload = HashMap::new();
        payload.insert("status".into(), Value::String("ok".into()));
        let env = Envelope::new(
            MessageType::Control,
            Command::Ack,
            self.source(),
            payload,
        )
        .with_correlation(correlation_id.to_string());
        self.send_envelope(writer, &env).await
    }

    async fn send_envelope(
        &self,
        writer: &Arc<Mutex<tokio::net::unix::OwnedWriteHalf>>,
        env: &Envelope,
    ) -> Result<()> {
        let mut data = serde_json::to_string(env)?;
        data.push('\n');
        writer.lock().await.write_all(data.as_bytes()).await?;
        Ok(())
    }

    async fn recv_envelope(
        &self,
        reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
    ) -> Result<Envelope> {
        let mut line = String::new();
        let n = reader.read_line(&mut line).await?;
        if n == 0 {
            return Err(AegisError::Connection("connection closed".into()));
        }
        Ok(serde_json::from_str(&line)?)
    }

    fn source(&self) -> String {
        format!("component:{}", self.cfg.component_name)
    }
}

enum LifecycleOutcome {
    Continue,
    Stop,
}