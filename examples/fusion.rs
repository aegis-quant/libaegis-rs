//! Unified component that works in both historical and realtime sessions.
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
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::info;

use libaegis::{Component, ComponentHandler, Config, DataStream, StreamMessage};
use libaegis::data_stream::{MarketData, OrderBook};
use libaegis::error::Result;

// ---------------------------------------------------------------------------
// Mode
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode { Realtime, Historical }

impl Mode {
    fn from_env() -> Self {
        match std::env::var("AEGIS_MODE").as_deref() {
            Ok("realtime") => Mode::Realtime,
            _              => Mode::Historical,
        }
    }
    fn as_str(self) -> &'static str {
        match self { Mode::Realtime => "realtime", Mode::Historical => "historical" }
    }
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

struct FusionHandler {
    socket_path: Arc<Mutex<String>>,
    mode:        Mode,
}

impl FusionHandler {
    fn new(mode: Mode) -> Self {
        Self { socket_path: Arc::new(Mutex::new(String::new())), mode }
    }
}

impl ComponentHandler for FusionHandler {
    async fn on_configure(&self, socket_path: String, topics: Vec<String>) -> Result<()> {
        info!(socket = %socket_path, ?topics, mode = self.mode.as_str(), "configure");
        *self.socket_path.lock().await = socket_path;
        Ok(())
    }

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

    // Called when a FINISHED session is restarted. Reset ALL per-run state.
    // on_running is NOT called again — the task above keeps running.
    async fn on_reborn(&self) {
        info!("reborn — resetting per-run state");
        // TODO: reset indicators, positions, counters, etc.
    }

    async fn on_ping(&self) { tracing::debug!("ping"); }

    async fn on_shutdown(&self) { info!("shutdown"); }

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
        if shutdown.is_cancelled() { break; }

        let mut stream = loop {
            if shutdown.is_cancelled() { break 'outer; }
            match DataStream::connect(&socket_path, &component_id, &session_id).await {
                Ok(s) => {
                    info!(topics = ?s.topics, "data stream connected");
                    break s;
                }
                Err(e) => {
                    tracing::warn!("data stream connect failed: {e} — retrying in 1s");
                    tokio::select! {
                        _ = shutdown.cancelled()                                          => break 'outer,
                        _ = tokio::time::sleep(std::time::Duration::from_secs(1))        => {}
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
                            tracing::warn!("data stream error: {e} — reconnecting");
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
        None => { tracing::warn!(topic = %msg.topic, "unparseable topic"); return; }
    };

    match data {
        // ── Common to both modes ─────────────────────────────────────────────

        MarketData::AggTrade(t) => {
            // historical: event_time == 0, symbol == "". Always use transact_time.
            info!(
                symbol     = %parts.symbol,
                ts         = t.transact_time,
                price      = t.price,
                qty        = t.quantity,
                normal_qty = t.normal_qty,   // 0 in historical
                is_maker   = t.is_buyer_maker,
                realtime   = t.is_realtime(),
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
            info!(
                symbol = %parts.symbol,
                tf     = parts.timeframe.as_deref().unwrap_or("?"),
                ts     = k.open_time,
                o      = k.open,
                h      = k.high,
                l      = k.low,
                c      = k.close,
                vol    = k.volume,
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
                symbol  = %parts.symbol,
                ts      = bd.timestamp,
                pct     = bd.percentage,
                depth   = bd.depth,
                notional = bd.notional,
                "bookDepth",
            );
        }

        MarketData::Metrics(m) => {
            if mode == Mode::Realtime {
                tracing::debug!(topic = %msg.topic, "metrics skipped (not available in realtime mode)");
                return;
            }
            info!(
                symbol   = %parts.symbol,
                ts       = m.create_time,
                oi       = m.sum_open_interest,
                oi_value = m.sum_open_interest_value,
                ls_ratio = m.count_long_short_ratio,
                "metrics",
            );
        }

        // ── Forward compatibility ────────────────────────────────────────────

        MarketData::Unknown { data_type, .. } => {
            tracing::warn!(topic = %msg.topic, %data_type, "unknown data type — SDK may need updating");
        }
    }
}

fn handle_order_book(symbol: &str, envelope_ts: i64, ob: OrderBook) {
    if !ob.has_real_timestamp() {
        tracing::debug!(
            symbol         = symbol,
            last_update_id = ob.last_update_id,
            "orderBook — spot stream (using last_update_id as timestamp proxy)",
        );
    }
    match (ob.best_bid(), ob.best_ask()) {
        (Some(bid), Some(ask)) => {
            info!(
                symbol     = symbol,
                ts         = envelope_ts,
                bid        = bid.price,
                ask        = ask.price,
                spread     = ask.price - bid.price,
                mid        = ob.mid_price().unwrap_or(0.0),
                bid_levels = ob.bids.len(),
                ask_levels = ob.asks.len(),
                "orderBook",
            );
        }
        _ => tracing::warn!(symbol = symbol, "orderBook snapshot has empty side"),
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
                .unwrap_or_else(|_| "libaegis=debug,fusion=info".into())
        )
        .init();

    info!(mode = mode.as_str(), "starting component");

    let socket_path   = std::env::var("AEGIS_SOCKET")
        .unwrap_or_else(|_| "/tmp/aegis-components.sock".into());
    let session_token = std::env::var("AEGIS_SESSION_TOKEN").unwrap_or_default();

    let mut cfg = Config::new(socket_path, session_token, "market_data");
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

    let component = Component::new(cfg, FusionHandler::new(mode));

    tokio::select! {
        res = component.run() => {
            if let Err(e) = res {
                eprintln!("component error: {e}");
                std::process::exit(1);
            }
        }
        _ = tokio::signal::ctrl_c() => { info!("ctrl-c — shutting down"); }
    }
    Ok(())
}
