use std::convert::Infallible;
use std::net::SocketAddr;
use std::num::NonZeroUsize;

use axum::Router;
use axum::extract::State;
use axum::response::sse::Event;
use axum::response::{Html, Sse};
use axum::routing::get;
use futures_util::Stream;
use triomphe::Arc;

use crate::monitor::State as MonitorState;

pub async fn start_server(port: u16, state: Arc<MonitorState>) {
    let app = Router::new()
        .route("/", get(root_handler))
        .route("/sse", get(sse_handler))
        .with_state(state);

    let address = SocketAddr::from(([0, 0, 0, 0], port));
    println!("Listening on http://{address}");
    let listener = compio::net::TcpListener::bind(address).await.unwrap();
    cyper_axum::serve(listener, app).await.unwrap();
}

async fn root_handler() -> Html<&'static str> {
    Html(include_str!("index.html"))
}

async fn sse_handler(
    State(state): State<Arc<MonitorState>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let (tx, rx) = z_queue::defaults::bounded(NonZeroUsize::MIN);

    let tx_clone = tx.clone();
    let handle = compio::runtime::spawn(async move {
        let tx = tx_clone;
        let _guard = state.add_listener();

        let mut prev_hash = [0u8; blake3::OUT_LEN];
        loop {
            let stats_listener = state.stats.observe();

            let json = {
                let stats = state.stats.latest_value_async().await;
                serde_json::to_string(&*stats).unwrap()
            };

            let hash = blake3::hash(json.as_bytes());
            if hash != prev_hash {
                prev_hash = *hash.as_bytes();

                let event = Event::default().event("stats").data(json);
                if tx.send_async(Ok(event)).await.is_err() {
                    break;
                }
            }

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
