//! Basic example: historical component that receives market data and resets
//! cleanly on session restart (REBORN).

use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::info;

use libaegis::{Component, ComponentHandler, Config, DataStream, StreamMessage};
use libaegis::data_stream::MarketData;
use libaegis::error::Result;

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

struct MarketDataHandler {
    socket_path: Arc<Mutex<String>>,
}

impl MarketDataHandler {
    fn new() -> Self {
        Self { socket_path: Arc::new(Mutex::new(String::new())) }
    }
}

impl ComponentHandler for MarketDataHandler {
    async fn on_configure(&self, socket_path: String, topics: Vec<String>) -> Result<()> {
        info!(socket = %socket_path, ?topics, "configure");
        *self.socket_path.lock().await = socket_path;
        Ok(())
    }

    // component_id and session_id are provided directly by the SDK from the
    // REGISTERED response — pass them straight to DataStream::connect.
    async fn on_running(
        &self,
        component_id: String,
        session_id:   String,
        shutdown:     CancellationToken,
    ) -> Result<()> {
        info!(%component_id, %session_id, "running");
        let socket = self.socket_path.lock().await.clone();
        tokio::spawn(async move {
            stream_worker(socket, component_id, session_id, shutdown).await;
        });
        Ok(())
    }

    // Called when Aegis restarts a FINISHED session (REBORN).
    // Reset ALL per-run state here. on_running is NOT called again — the
    // goroutine above keeps running and picks up data from the new orchestrator.
    async fn on_reborn(&self) {
        info!("reborn — resetting per-run state");
        // TODO: reset counters, positions, indicators, etc.
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
                        Ok((msg, data)) => handle_message(msg, data).await,
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
// Business logic
// ---------------------------------------------------------------------------

async fn handle_message(msg: StreamMessage, data: MarketData) {
    let parts = match msg.topic_parts() {
        Some(p) => p,
        None => { tracing::warn!(topic = %msg.topic, "unparseable topic"); return; }
    };

    match data {
        MarketData::AggTrade(t) => {
            info!(
                symbol   = %parts.symbol,
                ts       = t.transact_time,
                price    = t.price,
                qty      = t.quantity,
                is_maker = t.is_buyer_maker,
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
                symbol    = %parts.symbol,
                tf        = parts.timeframe.as_deref().unwrap_or("?"),
                ts        = k.open_time,
                o         = k.open,
                h         = k.high,
                l         = k.low,
                c         = k.close,
                vol       = k.volume,
                "kline",
            );
        }
        MarketData::BookDepth(bd) => {
            info!(
                symbol = %parts.symbol,
                ts     = bd.timestamp,
                pct    = bd.percentage,
                depth  = bd.depth,
                "bookDepth",
            );
        }
        MarketData::Metrics(m) => {
            info!(
                symbol = %parts.symbol,
                ts     = m.create_time,
                oi     = m.sum_open_interest,
                "metrics",
            );
        }
        MarketData::OrderBook(_) => {
            tracing::debug!(topic = %msg.topic, "orderBook (unexpected in historical mode)");
        }
        MarketData::Unknown { data_type, .. } => {
            tracing::warn!(topic = %msg.topic, %data_type, "unknown data type — SDK may need updating");
        }
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter("libaegis=debug,market_data=info")
        .init();

    let socket_path   = std::env::var("AEGIS_SOCKET")
        .unwrap_or_else(|_| "/tmp/aegis-components.sock".into());
    let session_token = std::env::var("AEGIS_SESSION_TOKEN").unwrap_or_default();

    let mut cfg = Config::new(socket_path, session_token, "market_data");
    cfg.version              = "0.1.0".into();
    cfg.supported_symbols    = vec!["BTCUSDT".into(), "ETHUSDT".into()];
    cfg.supported_timeframes = vec!["1m".into(), "5m".into()];
    cfg.requires_streams     = vec!["aggTrades".into(), "klines".into()];
    cfg.max_reconnect_attempts = 10;

    let component = Component::new(cfg, MarketDataHandler::new());

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