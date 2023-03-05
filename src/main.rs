use streams::{create_and_run_streams, stream_consumer, Event};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    let (tx, rx) = mpsc::unbounded_channel::<Event>();

    create_and_run_streams(3, 10.0, tx);
    stream_consumer(80, rx).await;
}
