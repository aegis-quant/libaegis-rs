use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use serde_json::Value;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
    sync::{Mutex, OnceCell},
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
    /// Store `socket_path` for use in `on_running`.
    fn on_configure(
        &self,
        socket_path: String,
        topics: Vec<String>,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    /// Called exactly once after the component first reaches RUNNING state.
    ///
    /// Start your main processing loop here (in a spawned task). `shutdown`
    /// is cancelled only on SHUTDOWN. On session restart Aegis sends REBORN
    /// instead — `on_running` is NOT called again. Reset per-run state in
    /// `on_reborn`; the existing task keeps running.
    fn on_running(
        &self,
        component_id: String,
        session_id:   String,
        shutdown:     CancellationToken,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    /// Called when Aegis sends REBORN (session restart).
    ///
    /// Reset ALL per-run state — positions, counters, buffers. The task
    /// started in `on_running` continues running unchanged. After this
    /// returns the SDK sends ACK.
    fn on_reborn(&self) -> impl std::future::Future<Output = ()> + Send {
        async {}
    }

    /// Called on every PING before the PONG is sent.
    fn on_ping(&self) -> impl std::future::Future<Output = ()> + Send {
        async {}
    }

    /// Called just before the component disconnects on SHUTDOWN.
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

    // Ensures on_running is called exactly once per process lifetime.
    // REBORN reuses the existing task — no second spawn.
    on_running_cell: OnceCell<()>,

    // Shutdown token shared across reconnects — cancelled only on SHUTDOWN.
    // Not cancelled on REBORN or reconnect.
    shutdown_token: CancellationToken,

    // last_ping tracks when we last received a PING from the daemon.
    // Shared with the watchdog task.
    last_ping: Arc<Mutex<Instant>>,
}

impl<H: ComponentHandler> Component<H> {
    pub fn new(cfg: Config, handler: H) -> Self {
        Self {
            cfg,
            handler:         Arc::new(handler),
            component_id:    Arc::new(Mutex::new(String::new())),
            session_id:      Arc::new(Mutex::new(String::new())),
            state:           Arc::new(Mutex::new(ComponentState::Init)),
            started_at:      Instant::now(),
            on_running_cell: OnceCell::new(),
            shutdown_token:  CancellationToken::new(),
            last_ping:       Arc::new(Mutex::new(Instant::now())),
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
        let session_token = if !self.cfg.session_token.is_empty() {
            self.cfg.session_token.clone()
        } else {
            std::env::var("AEGIS_SESSION_TOKEN").unwrap_or_default()
        };

        let mut payload = HashMap::new();
        payload.insert("session_token".into(),  Value::String(session_token));
        payload.insert("component_name".into(), Value::String(self.cfg.component_name.clone()));
        payload.insert("version".into(),        Value::String(self.cfg.version.clone()));

        // On reconnect reuse stored ID so the daemon can match the placeholder.
        // On first connect fall back to the AEGIS_COMPONENT_ID env var injected
        // by LaunchComponents.
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

        // Expect REGISTERED within a short deadline — if the daemon doesn't
        // respond in time it's not going to respond at all.
        let resp = self.recv_envelope_timeout(reader, Duration::from_secs(15)).await?;
        if resp.command == Command::RegistrationFailed {
            return Err(AegisError::Registration(
                resp.payload.get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("registration failed")
                    .to_string(),
            ));
        }
        if resp.command != Command::Registered {
            return Err(AegisError::Registration(format!(
                "unexpected response: {:?}", resp.command
            )));
        }

        *self.component_id.lock().await = resp.payload["component_id"]
            .as_str().unwrap_or_default().to_string();
        *self.session_id.lock().await = resp.payload["session_id"]
            .as_str().unwrap_or_default().to_string();
        *self.state.lock().await = ComponentState::Registered;

        info!(
            "Registered — component_id={} session_id={}",
            self.component_id.lock().await,
            self.session_id.lock().await,
        );

        // The daemon (WaitForReady) expects STATE_UPDATE(INITIALIZING) then
        // STATE_UPDATE(READY) before sending CONFIGURE. We also wait for the
        // daemon's ACK on each before proceeding.
        self.send_state_update(writer, ComponentState::Initializing, None).await?;
        self.recv_ack(reader).await?;
        self.send_state_update(writer, ComponentState::Ready, None).await?;
        self.recv_ack(reader).await?;

        Ok(())
    }

    // ------------------------------------------------------------------
    // Message loop — heartbeat design
    // ------------------------------------------------------------------
    //
    // The daemon's HeartbeatMonitor sends a PING every 5s to every
    // RUNNING/WAITING component. We respond with PONG and record the
    // arrival time in `self.last_ping`.
    //
    // A separate watchdog task checks `last_ping` every second. If no PING
    // has arrived within `cfg.ping_timeout` (default 25s) it cancels
    // `conn_token`, which causes the select! in the message loop to break
    // and return an error — triggering a reconnect.
    //
    // The watchdog only activates when the component is RUNNING or WAITING.
    // During handshake phases (INITIALIZING, READY, CONFIGURED) the daemon
    // does not send PINGs, so we seed `last_ping` on each state transition
    // to avoid a spurious watchdog trigger.
    //
    // There is NO per-message read timeout in the loop. A session can have
    // long periods with no market data (historical gap, trading halt) without
    // the control connection being dead. The only liveness signal we can rely
    // on is the daemon's PING.

    async fn message_loop(
        &self,
        writer: &Arc<Mutex<tokio::net::unix::OwnedWriteHalf>>,
        reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
    ) -> Result<()> {
        // conn_token is cancelled by the watchdog when PINGs stop arriving.
        // It is separate from shutdown_token (which is only cancelled on SHUTDOWN).
        let conn_token = CancellationToken::new();

        // Seed last_ping so the watchdog doesn't fire immediately on connect.
        *self.last_ping.lock().await = Instant::now();

        // Spawn the watchdog task.
        if self.cfg.ping_timeout > Duration::ZERO {
            let last_ping    = Arc::clone(&self.last_ping);
            let state        = Arc::clone(&self.state);
            let token        = conn_token.clone();
            let ping_timeout = self.cfg.ping_timeout;

            tokio::spawn(async move {
                ping_watchdog(last_ping, state, token, ping_timeout).await;
            });
        }

        loop {
            // No timeout here — conn_token handles liveness via the watchdog.
            let env = tokio::select! {
                // Watchdog fired — connection is dead.
                _ = conn_token.cancelled() => {
                    return Err(AegisError::Connection(
                        "ping watchdog: no PING received within timeout".into()
                    ));
                }
                // Daemon closed the connection.
                result = self.recv_envelope(reader) => result?,
            };

            debug!("Received type={:?} command={:?}", env.msg_type, env.command);

            match env.msg_type {
                MessageType::Heartbeat => {
                    self.handle_heartbeat(writer, &env).await?;
                }
                MessageType::Config => {
                    self.handle_config(writer, &env).await?;
                }
                MessageType::Lifecycle => {
                    match self.handle_lifecycle(writer, &env).await? {
                        LifecycleOutcome::Continue => {}
                        LifecycleOutcome::Stop => {
                            conn_token.cancel(); // stop the watchdog
                            return Ok(());
                        }
                    }
                }
                MessageType::Error => {
                    if self.handle_error_msg(&env).await {
                        conn_token.cancel();
                        return Err(AegisError::Connection("non-recoverable error from daemon".into()));
                    }
                }
                // ACKs for our STATE_UPDATEs — silently consumed.
                MessageType::Control => {
                    debug!("Control/{:?} — ignored", env.command);
                }
                _ => warn!("Unknown message type: {:?}", env.msg_type),
            }
        }
    }

    // ------------------------------------------------------------------
    // Handlers
    // ------------------------------------------------------------------

    async fn handle_heartbeat(
        &self,
        writer: &Arc<Mutex<tokio::net::unix::OwnedWriteHalf>>,
        env: &Envelope,
    ) -> Result<()> {
        if env.command != Command::Ping {
            debug!("Ignoring unexpected heartbeat command: {:?}", env.command);
            return Ok(());
        }

        // Record arrival time — resets the watchdog clock.
        *self.last_ping.lock().await = Instant::now();

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

        info!("Received CONFIGURE — socket={} topics={:?}", socket_path, topics);

        if let Err(e) = self.handler.on_configure(socket_path, topics).await {
            error!("on_configure error: {}", e);
            self.send_error(writer, "CONFIGURE_FAILED", &e.to_string(), true).await?;
            return Ok(());
        }

        // Protocol sequence after successful configuration:
        //   1. ACK  the CONFIGURE message
        //   2. STATE_UPDATE(Configured)
        //   3. STATE_UPDATE(Running)
        self.send_ack(writer, &env.message_id).await?;
        self.send_state_update(writer, ComponentState::Configured, None).await?;
        self.send_state_update(writer, ComponentState::Running, None).await?;

        info!("Component is RUNNING");

        // Seed the watchdog so it doesn't fire immediately on transition.
        *self.last_ping.lock().await = Instant::now();

        // on_running is called exactly once for the lifetime of this process.
        // On REBORN the same task continues — on_reborn resets its state.
        let initialized = self.on_running_cell.get().is_some();
        if !initialized {
            let handler      = Arc::clone(&self.handler);
            let component_id = self.component_id.lock().await.clone();
            let session_id   = self.session_id.lock().await.clone();
            let shutdown     = self.shutdown_token.clone();

            self.on_running_cell.get_or_init(|| async { () }).await;

            tokio::spawn(async move {
                if let Err(e) = handler.on_running(component_id, session_id, shutdown).await {
                    error!("on_running error: {}", e);
                }
            });
        }

        Ok(())
    }

    async fn handle_lifecycle(
        &self,
        writer: &Arc<Mutex<tokio::net::unix::OwnedWriteHalf>>,
        env: &Envelope,
    ) -> Result<LifecycleOutcome> {
        match env.command {
            Command::Reborn => {
                info!("REBORN — resetting per-run state");
                // Seed watchdog so the new run's first ping window starts fresh.
                *self.last_ping.lock().await = Instant::now();
                self.handler.on_reborn().await;
                self.send_ack(writer, &env.message_id).await?;
                info!("REBORN acknowledged — ready for new orchestrator");
                Ok(LifecycleOutcome::Continue)
            }

            Command::Shutdown => {
                info!("SHUTDOWN received — performing clean exit");
                self.shutdown_token.cancel();
                self.handler.on_shutdown().await;
                self.send_ack(writer, &env.message_id).await?;
                *self.state.lock().await = ComponentState::Shutdown;
                info!("Shutdown complete");
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
            .and_then(|v| v.as_str()).unwrap_or("UNKNOWN");
        let message = env.payload.get("message")
            .and_then(|v| v.as_str()).unwrap_or("");
        let recoverable = env.payload.get("recoverable")
            .and_then(|v| v.as_bool()).unwrap_or(false);

        error!("Error from daemon — code={} message={}", code, message);
        self.handler.on_error(code.to_string(), message.to_string()).await;
        !recoverable // true = fatal
    }

    // ------------------------------------------------------------------
    // Wire helpers
    // ------------------------------------------------------------------

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
            return Err(AegisError::Connection("connection closed by daemon".into()));
        }
        Ok(serde_json::from_str(&line)?)
    }

    /// Like `recv_envelope` but with a wall-clock deadline.
    /// Only used during the registration handshake — not in the message loop.
    async fn recv_envelope_timeout(
        &self,
        reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
        timeout: Duration,
    ) -> Result<Envelope> {
        tokio::time::timeout(timeout, self.recv_envelope(reader))
            .await
            .map_err(|_| AegisError::Timeout)?
    }

    /// Read and discard the next envelope, expecting it to be an ACK.
    /// Used during registration to consume the daemon's ACKs for
    /// STATE_UPDATE(INITIALIZING) and STATE_UPDATE(READY).
    async fn recv_ack(
        &self,
        reader: &mut BufReader<tokio::net::unix::OwnedReadHalf>,
    ) -> Result<()> {
        let env = self.recv_envelope_timeout(reader, Duration::from_secs(15)).await?;
        if env.command != Command::Ack {
            return Err(AegisError::Connection(format!(
                "expected ACK, got {:?}/{:?}", env.msg_type, env.command
            )));
        }
        Ok(())
    }

    fn source(&self) -> String {
        format!("component:{}", self.cfg.component_name)
    }
}

// ---------------------------------------------------------------------------
// Ping watchdog
// ---------------------------------------------------------------------------

/// Runs as a separate task. Polls `last_ping` every second and cancels
/// `conn_token` if no PING has arrived within `ping_timeout`.
///
/// Only enforces the deadline when the component is in a steady-state
/// (`Running` or `Waiting`). During handshake phases the daemon does not
/// send PINGs, so triggering there would be a false positive.
async fn ping_watchdog(
    last_ping:    Arc<Mutex<Instant>>,
    state:        Arc<Mutex<ComponentState>>,
    conn_token:   CancellationToken,
    ping_timeout: Duration,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            _ = conn_token.cancelled() => return,
            _ = interval.tick() => {}
        }

        // Only enforce during steady-state.
        let current_state = state.lock().await.clone();
        match current_state {
            ComponentState::Running | ComponentState::Waiting => {}
            _ => {
                // Not yet running — seed the timestamp so we don't fire
                // immediately when we transition to Running.
                *last_ping.lock().await = Instant::now();
                continue;
            }
        }

        let since = last_ping.lock().await.elapsed();
        if since > ping_timeout {
            warn!(
                "Ping watchdog: no PING received for {:.1}s (timeout: {:?}) — closing connection",
                since.as_secs_f64(),
                ping_timeout,
            );
            conn_token.cancel();
            return;
        }
    }
}

// ---------------------------------------------------------------------------
// Internal
// ---------------------------------------------------------------------------

enum LifecycleOutcome {
    Continue,
    Stop,
}
