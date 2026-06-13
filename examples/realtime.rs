//! Realtime example: subscribes to live Binance market data and resets
//! cleanly on session restart (REBORN).

use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::info;

use libaegis::{Component, ComponentHandler, Config, DataStream, StreamMessage};
use libaegis::data_stream::{MarketData, OrderBook};
use libaegis::error::Result;

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

struct RealtimeHandler {
    socket_path: Arc<Mutex<String>>,
}

impl RealtimeHandler {
    fn new() -> Self {
        Self { socket_path: Arc::new(Mutex::new(String::new())) }
    }
}

impl ComponentHandler for RealtimeHandler {
    async fn on_configure(&self, socket_path: String, topics: Vec<String>) -> Result<()> {
        info!(socket = %socket_path, ?topics, "configure");
        *self.socket_path.lock().await = socket_path;
        Ok(())
    }

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

    async fn on_reborn(&self) {
        info!("reborn — resetting per-run state");
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
                symbol      = %parts.symbol,
                ts          = t.transact_time,
                price       = t.price,
                qty         = t.quantity,
                normal_qty  = t.normal_qty,
                is_maker    = t.is_buyer_maker,
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
        MarketData::OrderBook(ob) => handle_order_book(&parts.symbol, msg.ts, ob),
        MarketData::BookDepth(_) => {
            tracing::debug!(topic = %msg.topic, "bookDepth (not available in realtime mode)");
        }
        MarketData::Metrics(_) => {
            tracing::debug!(topic = %msg.topic, "metrics (not available in realtime mode)");
        }
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
    tracing_subscriber::fmt()
        .with_env_filter("libaegis=debug,realtime=info")
        .init();

    let socket_path   = std::env::var("AEGIS_SOCKET")
        .unwrap_or_else(|_| "/tmp/aegis-components.sock".into());
    let session_token = std::env::var("AEGIS_SESSION_TOKEN").unwrap_or_default();

    let mut cfg = Config::new(socket_path, session_token, "realtime");
    cfg.version                    = "0.1.0".into();
    cfg.supported_symbols          = vec!["BTCUSDT".into()];
    cfg.supported_timeframes       = vec!["1m".into(), "5m".into()];
    cfg.requires_streams           = vec!["aggTrades".into(), "klines".into(), "orderBook".into()];
    cfg.supported_orderbook_speeds = vec!["100ms".into()];
    cfg.max_reconnect_attempts     = 10;

    let component = Component::new(cfg, RealtimeHandler::new());

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
