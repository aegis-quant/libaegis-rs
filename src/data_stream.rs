//! DataStream — client for the Aegis data stream Unix socket.
//!
//! The orchestrator publishes market data as newline-delimited JSON frames
//! delivered over a Unix socket separate from the control channel. The
//! component must complete a handshake before data starts flowing:
//!
//!   Component → {"component_id": "cmp-...", "session_token": "<session_id>"}
//!   Server    → {"status": "ok", "topics": ["aegis.<sid>.klines.BTCUSDT.1m", ...]}
//!
//! Every frame is a JSON object:
//!   {"session_id":"...","topic":"aegis.<sid>.<type>.<sym>[.<tf>]","ts":1234,"data":{...}}
//!
//! Use [`StreamMessage::parse`] to decode `data` into a typed [`MarketData`] variant.
//!
//! ## Topic format
//! `aegis.<session_id>.<data_type>.<symbol>[.<timeframe>]`
//! Use [`TopicParts::parse`] to break it apart.
//!
//! ## AggTrade note
//! In historical mode `event_time == 0` and `symbol` is empty — these
//! fields are absent from Binance Vision CSV files. Always use
//! `transact_time` as the canonical timestamp (equals the envelope `ts`).
//!
//! ## OrderBook note
//! Binance USD-M futures depth streams include `event_time` (field `"T"`).
//! Spot depth streams do **not** — the server falls back to `last_update_id`
//! as a monotonic proxy. Use [`OrderBook::has_real_timestamp`] to detect this.
//!
//! ## Liveness
//! [`DataStream::next`] does **not** apply a read deadline. A session can
//! have long periods with no market data (historical gap, trading halt) without
//! the socket being dead. Liveness is guaranteed indirectly: if the daemon dies,
//! the component's ping watchdog closes the control socket, which the application
//! uses as a signal to also reconnect the data stream.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::UnixStream,
};

use crate::error::{AegisError, Result};

// ─── Wire format ──────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct RawEnvelope {
    pub session_id: String,
    pub topic:      String,
    pub ts:         i64,
    pub data:       Value,
}

#[derive(Serialize)]
struct Handshake<'a> {
    component_id:  &'a str,
    session_token: &'a str,
}

#[derive(Deserialize)]
struct HandshakeResponse {
    status:  String,
    message: Option<String>,
    topics:  Option<Vec<String>>,
}

// ─── Public types ─────────────────────────────────────────────────────────────

/// A single decoded message from the data stream.
#[derive(Debug, Clone)]
pub struct StreamMessage {
    pub session_id: String,
    /// Full NATS topic: `aegis.<session_id>.<data_type>.<symbol>[.<timeframe>]`
    pub topic:      String,
    /// Canonical unix-ms timestamp for this row (see module docs for caveats).
    pub ts:         i64,
    /// Raw JSON payload.
    pub data:       Value,
}

impl StreamMessage {
    /// Parse the topic into its component parts. Returns `None` on malformed topics.
    pub fn topic_parts(&self) -> Option<TopicParts> {
        TopicParts::parse(&self.topic)
    }

    /// Decode the `data` payload into a typed [`MarketData`] variant.
    ///
    /// Returns [`MarketData::Unknown`] for unrecognised data types — forward
    /// compatibility when the server adds new types.
    pub fn parse(&self) -> Result<MarketData> {
        let parts = self.topic_parts()
            .ok_or_else(|| AegisError::Connection(format!("unparseable topic: {}", self.topic)))?;
        MarketData::decode(&parts.data_type, self.data.clone())
    }
}

/// Components of a full NATS topic string.
///
/// Format: `aegis.<session_id>.<data_type>.<symbol>[.<timeframe>]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicParts {
    pub session_id: String,
    pub data_type:  String,
    pub symbol:     String,
    /// `None` for flat types (trades, aggTrades, bookDepth, metrics).
    pub timeframe:  Option<String>,
}

impl TopicParts {
    pub fn parse(topic: &str) -> Option<Self> {
        let parts: Vec<&str> = topic.splitn(6, '.').collect();
        if parts.len() < 4 {
            return None;
        }
        Some(TopicParts {
            session_id: parts[1].to_string(),
            data_type:  parts[2].to_string(),
            symbol:     parts[3].to_string(),
            timeframe:  parts.get(4).map(|s| s.to_string()),
        })
    }
}

// ─── Typed payloads ───────────────────────────────────────────────────────────

/// Aggregated trade from Binance.
///
/// Historical vs realtime differences:
/// - `event_time`: 0 in historical mode (not in CSV).
/// - `symbol`: empty string in historical mode.
/// - `normal_qty`: 0.0 in historical mode (RPI-adjusted qty, futures only).
///
/// Always use `transact_time` as the canonical timestamp.
#[derive(Debug, Clone, Deserialize)]
pub struct AggTrade {
    /// Unix ms. Zero in historical mode.
    pub event_time:     i64,
    /// Empty in historical mode.
    pub symbol:         String,
    pub agg_trade_id:   i64,
    pub price:          f64,
    /// Total quantity including RPI orders.
    pub quantity:       f64,
    /// Quantity excluding RPI orders (futures only). Zero in historical mode.
    pub normal_qty:     f64,
    pub first_trade_id: i64,
    pub last_trade_id:  i64,
    /// Canonical timestamp in both modes. Equals the envelope `ts`.
    pub transact_time:  i64,
    pub is_buyer_maker: bool,
}

impl AggTrade {
    /// `true` if this row came from a live WebSocket stream (event_time != 0).
    #[inline]
    pub fn is_realtime(&self) -> bool {
        self.event_time != 0
    }
}

/// Individual raw trade.
#[derive(Debug, Clone, Deserialize)]
pub struct Trade {
    pub id:             i64,
    pub price:          f64,
    pub qty:            f64,
    pub quote_qty:      f64,
    /// Unix ms. Always valid.
    pub time:           i64,
    pub is_buyer_maker: bool,
}

/// OHLCV kline / candlestick.
#[derive(Debug, Clone, Deserialize)]
pub struct Kline {
    /// Unix ms. Always valid.
    pub open_time:              i64,
    pub open:                   f64,
    pub high:                   f64,
    pub low:                    f64,
    pub close:                  f64,
    pub volume:                 f64,
    pub close_time:             i64,
    pub quote_volume:           f64,
    pub count:                  i64,
    pub taker_buy_volume:       f64,
    pub taker_buy_quote_volume: f64,
}

/// Partial-depth order book update (realtime only, no CSV equivalent).
///
/// Binance USD-M futures streams include `event_time` (`"T"`).
/// Binance spot streams do not — the server stores `last_update_id` in
/// `event_time` as a monotonic proxy. Use [`has_real_timestamp`][OrderBook::has_real_timestamp].
///
/// Bids and asks are incremental updates. Levels with `quantity == 0.0`
/// indicate removal. Maintain a local book and apply each update.
#[derive(Debug, Clone, Deserialize)]
pub struct OrderBook {
    pub last_update_id: i64,
    /// Unix ms for USD-M futures streams. `last_update_id` for spot streams.
    pub event_time:     i64,
    pub bids:           Vec<PriceLevel>,
    pub asks:           Vec<PriceLevel>,
}

impl OrderBook {
    /// `true` when `event_time` is a real unix-ms timestamp.
    /// Values > 1_000_000_000_000 (year 2001) are real timestamps.
    #[inline]
    pub fn has_real_timestamp(&self) -> bool {
        self.event_time > 1_000_000_000_000
    }

    /// Best bid (highest price). `None` if the snapshot is empty.
    pub fn best_bid(&self) -> Option<&PriceLevel> {
        self.bids.first()
    }

    /// Best ask (lowest price). `None` if the snapshot is empty.
    pub fn best_ask(&self) -> Option<&PriceLevel> {
        self.asks.first()
    }

    /// Mid-price between best bid and ask. `None` if either side is empty.
    pub fn mid_price(&self) -> Option<f64> {
        Some((self.best_bid()?.price + self.best_ask()?.price) / 2.0)
    }
}

/// Single bid or ask level.
#[derive(Debug, Clone, Deserialize)]
pub struct PriceLevel {
    pub price:    f64,
    pub quantity: f64,
}

/// Aggregated order book depth snapshot (historical only).
#[derive(Debug, Clone, Deserialize)]
pub struct BookDepth {
    /// Unix ms. Always valid.
    pub timestamp:  i64,
    pub percentage: f64,
    pub depth:      f64,
    pub notional:   f64,
}

/// Funding-rate / open-interest metrics snapshot (historical only).
#[derive(Debug, Clone, Deserialize)]
pub struct Metrics {
    /// Unix ms. Always valid.
    pub create_time:                      i64,
    pub symbol:                           String,
    pub sum_open_interest:                f64,
    pub sum_open_interest_value:          f64,
    pub count_toptrader_long_short_ratio: f64,
    pub sum_toptrader_long_short_ratio:   f64,
    pub count_long_short_ratio:           f64,
    pub sum_taker_long_short_vol_ratio:   f64,
}

/// Typed market data payload.
#[derive(Debug, Clone)]
pub enum MarketData {
    AggTrade(AggTrade),
    Trade(Trade),
    Kline(Kline),
    /// Realtime only — no CSV/historical equivalent.
    OrderBook(OrderBook),
    /// Historical only — no WebSocket/realtime equivalent.
    BookDepth(BookDepth),
    /// Historical only — no WebSocket/realtime equivalent.
    Metrics(Metrics),
    /// Forward-compatibility variant for data types added after this SDK version.
    Unknown {
        data_type: String,
        payload:   Value,
    },
}

impl MarketData {
    fn decode(data_type: &str, payload: Value) -> Result<Self> {
        match data_type {
            "aggTrades" => Ok(MarketData::AggTrade(serde_json::from_value(payload)?)),
            "trades"    => Ok(MarketData::Trade(serde_json::from_value(payload)?)),
            "klines"    => Ok(MarketData::Kline(serde_json::from_value(payload)?)),
            "orderBook" => Ok(MarketData::OrderBook(serde_json::from_value(payload)?)),
            "bookDepth" => Ok(MarketData::BookDepth(serde_json::from_value(payload)?)),
            "metrics"   => Ok(MarketData::Metrics(serde_json::from_value(payload)?)),
            other       => Ok(MarketData::Unknown {
                data_type: other.to_string(),
                payload,
            }),
        }
    }
}

// ─── DataStream ───────────────────────────────────────────────────────────────

/// Active connection to the Aegis data stream socket.
///
/// Connect after receiving the CONFIGURE message, using the `component_id`
/// and `session_id` provided in `on_running`:
///
/// ```rust,ignore
/// let mut stream = DataStream::connect(&socket_path, &component_id, &session_id).await?;
/// loop {
///     match stream.next_parsed().await {
///         Ok((msg, data)) => { /* process */ }
///         Err(e) => { break; } // reconnect
///     }
/// }
/// ```
///
/// The data stream and the control channel are two independent Unix sockets.
/// Closing one does not affect the other.
pub struct DataStream {
    /// Topics this stream is subscribed to (from the handshake response).
    pub topics: Vec<String>,
    reader:     BufReader<tokio::net::unix::OwnedReadHalf>,
    // We hold the write half alive to keep the connection open.
    // The handshake is the only write; after that it's read-only.
    _writer:    tokio::net::unix::OwnedWriteHalf,
}

impl DataStream {
    /// Connect to the data stream socket and complete the handshake.
    ///
    /// - `socket_path`  — from the CONFIGURE payload (`data_stream_socket`).
    /// - `component_id` — from `on_running`'s first argument.
    /// - `session_id`   — from `on_running`'s second argument.
    pub async fn connect(
        socket_path:  &str,
        component_id: &str,
        session_id:   &str,
    ) -> Result<Self> {
        let stream = UnixStream::connect(socket_path).await
            .map_err(|e| AegisError::Connection(format!("data stream connect {socket_path}: {e}")))?;

        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        // Send handshake.
        let hs = Handshake { component_id, session_token: session_id };
        let mut frame = serde_json::to_string(&hs)?;
        frame.push('\n');
        write_half.write_all(frame.as_bytes()).await.map_err(AegisError::Io)?;

        // Read handshake response with a short deadline — Unix domain sockets
        // on localhost respond in microseconds; 3s is generous while still
        // recovering quickly when the server is momentarily busy.
        let mut line = String::new();
        tokio::time::timeout(Duration::from_secs(3), reader.read_line(&mut line))
            .await
            .map_err(|_| AegisError::Timeout)?
            .map_err(AegisError::Io)?;

        let resp: HandshakeResponse = serde_json::from_str(&line)
            .map_err(|e| AegisError::Connection(format!("handshake response parse: {e}")))?;

        if resp.status != "ok" {
            return Err(AegisError::Connection(format!(
                "data stream handshake rejected: {}",
                resp.message.unwrap_or_else(|| "no message".into()),
            )));
        }

        tracing::debug!(topics = ?resp.topics, "data stream handshake OK");

        Ok(Self {
            topics:  resp.topics.unwrap_or_default(),
            reader,
            _writer: write_half,
        })
    }

    /// Read the next raw message from the stream.
    ///
    /// Blocks until a frame arrives or the connection closes. Returns
    /// `Err` on EOF or any I/O error — the caller should reconnect.
    ///
    /// **No read deadline is applied.** A session can legitimately have
    /// long quiet periods (historical gap, trading halt) without the socket
    /// being dead. Liveness is managed by the control channel's ping watchdog.
    pub async fn next(&mut self) -> Result<StreamMessage> {
        let mut line = String::new();
        let n = self.reader.read_line(&mut line).await.map_err(AegisError::Io)?;
        if n == 0 {
            return Err(AegisError::Connection("data stream closed by server".into()));
        }
        let env: RawEnvelope = serde_json::from_str(&line)?;
        if env.topic.is_empty() {
            return Err(AegisError::Connection("received empty topic in frame".into()));
        }
        Ok(StreamMessage {
            session_id: env.session_id,
            topic:      env.topic,
            ts:         env.ts,
            data:       env.data,
        })
    }

    /// Read the next message and decode the payload in one call.
    ///
    /// Equivalent to `stream.next().await` followed by `msg.parse()`.
    pub async fn next_parsed(&mut self) -> Result<(StreamMessage, MarketData)> {
        let msg  = self.next().await?;
        let data = msg.parse()?;
        Ok((msg, data))
    }
}
