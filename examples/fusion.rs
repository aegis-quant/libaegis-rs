//! Aegis component — works in both historical and realtime sessions.
//!
//! Set `AEGIS_MODE=historical` or `AEGIS_MODE=realtime` (default: realtime).
//!
//! Data availability per mode:
//!
//! | Stream      | historical | realtime |
//! |-------------|:----------:|:--------:|
//! | aggTrades   |     ✓      |    ✓     |
//! | trades      |     ✓      |    ✓     |
//! | klines      |     ✓      |    ✓     |
//! | orderBook   |     –      |    ✓     |
//! | bookDepth   |     ✓      |    –     |
//! | metrics     |     ✓      |    –     |

use std::sync::Arc;

use libaegis::{Component, ComponentHandler, Config, DataStream, StreamMessage};
use libaegis::data_stream::{MarketData, OrderBook};
use libaegis::error::Result;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::info;

// ---------------------------------------------------------------------------
// Mode
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Realtime,
    Historical,
}

impl Mode {
    fn from_env() -> Self {
        match std::env::var("AEGIS_MODE").as_deref() {
            Ok("realtime") => Mode::Realtime,
            _                => Mode::Historical,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Mode::Realtime   => "realtime",
            Mode::Historical => "historical",
        }
    }
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

struct ComponentHandlerImpl {
    socket_path: Arc<Mutex<String>>,
    mode:        Mode,
}

impl ComponentHandlerImpl {
    fn new(mode: Mode) -> Self {
        Self {
            socket_path: Arc::new(Mutex::new(String::new())),
            mode,
        }
    }
}

impl ComponentHandler for ComponentHandlerImpl {
    async fn on_configure(&self, socket_path: String, topics: Vec<String>) -> Result<()> {
        info!(socket = %socket_path, ?topics, mode = self.mode.as_str(), "configure");
        *self.socket_path.lock().await = socket_path;
        Ok(())
    }

    // component_id and session_id are passed directly by the SDK — populated
    // from the REGISTERED response. No need to store or seed them manually.
    async fn on_running(
        &self,
        component_id: String,
        session_id:   String,
        shutdown:     CancellationToken,
    ) -> Result<()> {
        info!(%component_id, %session_id, mode = self.mode.as_str(), "running");

        let socket = self.socket_path.lock().await.clone();
        let mode   = self.mode;

        tokio::spawn(async move {
            stream_worker(socket, component_id, session_id, mode, shutdown).await;
        });

        Ok(())
    }

    async fn on_reborn(&self) {
        // Reset ALL per-run state here — counters, positions, buffers, etc.
        // on_running is NOT called again; the existing worker task keeps running
        // and will receive data from the new orchestrator automatically.
        info!("reborn — resetting per-run state");
    }

    async fn on_ping(&self) {
        tracing::debug!("ping");
    }

    async fn on_shutdown(&self) {
        info!("shutdown — releasing resources");
    }

    async fn on_error(&self, code: String, message: String) {
        tracing::error!(%code, %message, "aegis error");
    }
}

// ---------------------------------------------------------------------------
// Stream worker
// ---------------------------------------------------------------------------

async fn stream_worker(
    socket_path:  String,
    component_id: String,
    session_id:   String,
    mode:         Mode,
    shutdown:     CancellationToken,
) {
    'outer: loop {
        if shutdown.is_cancelled() {
            break;
        }

        let mut stream = loop {
            if shutdown.is_cancelled() {
                break 'outer;
            }
            match DataStream::connect(&socket_path, &component_id, &session_id).await {
                Ok(s) => {
                    info!(topics = ?s.topics, "data stream connected");
                    break s;
                }
                Err(e) => {
                    tracing::warn!("data stream connect error: {e} — retrying in 1s");
                    tokio::select! {
                        _ = shutdown.cancelled()                                      => break 'outer,
                        _ = tokio::time::sleep(std::time::Duration::from_secs(1))    => {}
                    }
                }
            }
        };

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break 'outer,
                result = stream.next_parsed() => {
                    match result {
                        Ok((msg, data)) => handle_message(msg, data, mode).await,
                        Err(e) => {
                            tracing::warn!("data stream closed: {e} — reconnecting");
                            break;
                        }
                    }
                }
            }
        }

        tokio::select! {
            _ = shutdown.cancelled()                                               => break 'outer,
            _ = tokio::time::sleep(std::time::Duration::from_millis(500))         => {}
        }
    }

    info!("stream worker stopped");
}

// ---------------------------------------------------------------------------
// Unified message handler
// ---------------------------------------------------------------------------

async fn handle_message(msg: StreamMessage, data: MarketData, mode: Mode) {
    let parts = match msg.topic_parts() {
        Some(p) => p,
        None => {
            tracing::warn!(topic = %msg.topic, "unparseable topic");
            return;
        }
    };

    match data {
        // ── Common to both modes ─────────────────────────────────────────────

        MarketData::AggTrade(t) => {
            // historical: event_time == 0, symbol == "". Use transact_time always.
            info!(
                symbol      = %parts.symbol,
                ts          = t.transact_time,
                price       = t.price,
                qty         = t.quantity,
                normal_qty  = t.normal_qty,   // 0 in historical mode
                is_maker    = t.is_buyer_maker,
                realtime    = t.is_realtime(),
                "aggTrade",
            );
        }

        MarketData::Trade(t) => {
            info!(
                symbol   = %parts.symbol,
                ts       = t.time,
                price    = t.price,
                qty      = t.qty,
                is_maker = t.is_buyer_maker,
                "trade",
            );
        }

        MarketData::Kline(k) => {
            let timeframe = parts.timeframe.as_deref().unwrap_or("?");
            info!(
                symbol    = %parts.symbol,
                timeframe = timeframe,
                ts        = k.open_time,
                open      = k.open,
                high      = k.high,
                low       = k.low,
                close     = k.close,
                volume    = k.volume,
                "kline",
            );
        }

        // ── Realtime-only ────────────────────────────────────────────────────

        MarketData::OrderBook(ob) => {
            if mode == Mode::Historical {
                tracing::debug!(topic = %msg.topic, "orderBook skipped (not available in historical mode)");
                return;
            }
            handle_order_book(&parts.symbol, msg.ts, ob);
        }

        // ── Historical-only ──────────────────────────────────────────────────

        MarketData::BookDepth(bd) => {
            if mode == Mode::Realtime {
                tracing::debug!(topic = %msg.topic, "bookDepth skipped (not available in realtime mode)");
                return;
            }
            info!(
                symbol     = %parts.symbol,
                ts         = bd.timestamp,
                percentage = bd.percentage,
                depth      = bd.depth,
                notional   = bd.notional,
                "bookDepth",
            );
        }

        MarketData::Metrics(m) => {
            if mode == Mode::Realtime {
                tracing::debug!(topic = %msg.topic, "metrics skipped (not available in realtime mode)");
                return;
            }
            info!(
                symbol       = %parts.symbol,
                ts           = m.create_time,
                oi           = m.sum_open_interest,
                oi_value     = m.sum_open_interest_value,
                long_short   = m.count_long_short_ratio,
                "metrics",
            );
        }

        // ── Forward-compat ───────────────────────────────────────────────────

        MarketData::Unknown { data_type, .. } => {
            tracing::warn!(
                topic     = %msg.topic,
                data_type = %data_type,
                "unknown data type — SDK may need updating",
            );
        }
    }
}

fn handle_order_book(symbol: &str, envelope_ts: i64, ob: OrderBook) {
    if !ob.has_real_timestamp() {
        tracing::debug!(
            symbol         = symbol,
            last_update_id = ob.last_update_id,
            "orderBook — spot stream (no real timestamp, using last_update_id)",
        );
    }

    match (ob.best_bid(), ob.best_ask()) {
        (Some(bid), Some(ask)) => {
            let spread = ask.price - bid.price;
            let mid    = ob.mid_price().unwrap_or(0.0);
            info!(
                symbol    = symbol,
                ts        = envelope_ts,
                bid       = bid.price,
                ask       = ask.price,
                spread    = spread,
                mid_price = mid,
                bid_depth = ob.bids.len(),
                ask_depth = ob.asks.len(),
                "orderBook",
            );
        }
        _ => tracing::warn!(symbol = symbol, "orderBook snapshot has empty bids or asks"),
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let mode = Mode::from_env();

    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "libaegis=debug,market_data=info".into())
        )
        .init();

    info!(mode = mode.as_str(), "starting component");

    // socket_path falls back to AEGIS_SOCKET env var inside Config::new when
    // passed as empty string — or you can pass it explicitly here.
    // session_token is injected by the Aegis daemon via AEGIS_SESSION_TOKEN
    // and read inside component.rs register() — pass "" here, same as Go SDK.
    let socket_path = std::env::var("AEGIS_SOCKET")
        .unwrap_or_else(|_| "/tmp/aegis-components.sock".into());

    let mut cfg = Config::new(socket_path, "", "market_data");
    cfg.version              = "0.1.0".into();
    cfg.supported_symbols    = vec!["BTCUSDT".into(), "ETHUSDT".into()];
    cfg.supported_timeframes = vec!["1m".into(), "5m".into()];
    cfg.max_reconnect_attempts = 10;

    match mode {
        Mode::Historical => {
            cfg.requires_streams = vec![
                "aggTrades".into(),
                "klines".into(),
                "bookDepth".into(),
                "metrics".into(),
            ];
        }
        Mode::Realtime => {
            cfg.requires_streams = vec![
                "aggTrades".into(),
                "klines".into(),
                "orderBook".into(),
            ];
            cfg.supported_orderbook_speeds = vec!["100ms".into()];
        }
    }

    let component = Component::new(cfg, ComponentHandlerImpl::new(mode));

    tokio::select! {
        res = component.run() => {
            if let Err(e) = res {
                eprintln!("component stopped with error: {e}");
                std::process::exit(1);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("ctrl-c — shutting down");
        }
    }

    Ok(())
}