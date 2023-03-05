use async_stream::stream;
use futures::{pin_mut, Stream, StreamExt};
use rand_distr::{Distribution, Poisson};
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    time::{sleep, Duration, Instant},
};

pub struct Event {
    pub enqueued_time: Instant,
}

impl Event {
    pub fn new(now: Instant) -> Self {
        Self { enqueued_time: now }
    }
}

pub fn create_and_run_streams(
    number_of_streams: i32,
    expected_events_per_minute_per_stream: f64,
    tx: UnboundedSender<Event>,
) {
    for _ in 0..number_of_streams {
        run_stream(expected_events_per_minute_per_stream, tx.clone());
    }
}

pub async fn stream_consumer(work_millis: u64, mut rx: UnboundedReceiver<Event>) {
    let mut deltas = vec![];

    while let Some(evt) = rx.recv().await {
        let now = Instant::now();
        let enqueued = evt.enqueued_time;
        let delta = now - enqueued;
        deltas.push(delta);

        println!("In queue time: {:?}", delta);
        sleep(Duration::from_millis(work_millis)).await;
    }

    let len = deltas.len();
    println!(
        "Avg queued time: {:?}ms",
        deltas.iter().map(|d| d.as_millis()).sum::<u128>() / (len as u128)
    )
}

fn create_stream(expected_events_per_minute: f64) -> impl Stream<Item = Event> {
    let poi = Poisson::new(expected_events_per_minute).unwrap();
    let sleep_time = move || {
        let mut sample = poi.sample(&mut rand::thread_rng());
        if sample <= 0.0 {
            sample = 1.0;
        }

        60.0 / sample
    };

    stream! {
        for _ in 0..60 {
            sleep(Duration::from_secs(sleep_time() as u64)).await;
            yield Event::new(Instant::now());
        }
    }
}

fn run_stream(expected_events_per_minute: f64, tx: UnboundedSender<Event>) {
    tokio::spawn(async move {
        let stream = create_stream(expected_events_per_minute);
        pin_mut!(stream);

        while let Some(evt) = stream.next().await {
            let _ = tx.send(evt);
        }
    });
}
