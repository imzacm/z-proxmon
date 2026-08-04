use std::convert::Infallible;
use std::net::SocketAddr;
use std::num::NonZeroUsize;

use axum::Router;
use axum::extract::{Query, State};
use axum::response::sse::Event;
use axum::response::{Html, Sse};
use axum::routing::get;
use futures_util::Stream;
use serde::Deserialize;
use triomphe::Arc;

use crate::monitor::State as MonitorState;
use crate::monitor_log::MonitorLog;

#[derive(Deserialize)]
struct SseQuery {
    history_ms: Option<u64>,
}

#[derive(Clone)]
struct ServerState {
    state: Arc<MonitorState>,
    monitor_log: MonitorLog,
}

unsafe impl Send for ServerState {}
unsafe impl Sync for ServerState {}

pub async fn start_server(port: u16, state: Arc<MonitorState>, monitor_log: MonitorLog) {
    let app = Router::new()
        .route("/", get(root_handler))
        .route("/sse", get(sse_handler))
        .with_state(ServerState { state, monitor_log });

    let address = SocketAddr::from(([0, 0, 0, 0], port));
    println!("Listening on http://{address}");
    let listener = compio::net::TcpListener::bind(address).await.unwrap();
    cyper_axum::serve(listener, app).await.unwrap();
}

async fn root_handler() -> Html<&'static str> {
    Html(include_str!("index.html"))
}

async fn sse_handler(
    State(ServerState { state, monitor_log }): State<ServerState>,
    Query(query): Query<SseQuery>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let (tx, rx) = z_queue::defaults::bounded(NonZeroUsize::MIN);

    // Default to 1 minute (60,000 ms).
    let history_ms = query.history_ms.unwrap_or(60_000);
    let history_ms: i64 = history_ms.try_into().expect("history_ms is too large");
    let history_min_time =
        time::OffsetDateTime::now_utc() - time::Duration::milliseconds(history_ms);

    let tx_clone = tx.clone();
    let handle = compio::runtime::spawn(async move {
        let tx = tx_clone;
        let _guard = state.add_listener();

        let mut prev_hash = [0u8; blake3::OUT_LEN];
        let mut json = String::new();

        {
            let mut history = Vec::with_capacity(1000);

            let mut cursor = monitor_log.cursor().await.expect("Failed to create log cursor");
            cursor.skip_to_hour(history_min_time).await;

            loop {
                let finished = match cursor.next().await.expect("Failed to read log") {
                    Some(stats) => {
                        if stats.updated_at.is_some_and(|t| t >= history_min_time) {
                            history.push(stats);
                        }
                        false
                    }
                    None => true,
                };

                if history.len() == 1000 || finished {
                    let mut bytes = json.into_bytes();
                    serde_json::to_writer(&mut bytes, &history).unwrap();
                    json = unsafe { String::from_utf8_unchecked(bytes) };

                    let event = Event::default().event("history").data(&json);

                    json.clear();
                    history.clear();

                    if tx.send_async(Ok(event)).await.is_err() {
                        break;
                    }
                }

                if finished {
                    break;
                }
            }
        }

        loop {
            let stats_listener = state.stats.observe();

            {
                let stats = state.stats.latest_value();
                let mut bytes = json.into_bytes();
                serde_json::to_writer(&mut bytes, &*stats).unwrap();
                json = unsafe { String::from_utf8_unchecked(bytes) };
            }

            let hash = blake3::hash(json.as_bytes());
            if hash != prev_hash {
                prev_hash = *hash.as_bytes();

                let event = Event::default().event("stats").data(&json);
                if tx.send_async(Ok(event)).await.is_err() {
                    break;
                }
            }

            json.clear();

            stats_listener.await;
        }
    });

    compio::runtime::spawn(async move {
        let mut interval = compio::time::interval(std::time::Duration::from_secs(15));
        loop {
            interval.tick().await;
            // The browser ignores this, but it keeps the TCP socket warm.
            let event = Event::default().comment("keep-alive");
            if tx.send_async(Ok(event)).await.is_err() {
                break;
            }
        }

        handle.cancel().await;
    })
    .detach();

    let stream = rx.into_stream();
    Sse::new(stream)
}
