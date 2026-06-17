//! Gracio demo: spawn, sleep, and channel all together.
//!
//! Run with:
//! ```bash
//! cargo run --example demo
//! ```

use gracio::{bounded_channel, run_blocking, sleep, spawn};
use std::time::{Duration, Instant};

fn main() {
    let start = Instant::now();

    run_blocking(async {
        // Spawn a producer task.
        let (tx, rx) = bounded_channel(4);
        let producer = spawn(async move {
            for i in 1..=5 {
                println!("[producer] sending {i}");
                tx.send(i).await;
                sleep(Duration::from_millis(20)).await;
            }
        });

        // Spawn a consumer task.
        let consumer = spawn(async move {
            for _ in 1..=5 {
                let val = rx.recv().await;
                println!("[consumer] received {val}");
                sleep(Duration::from_millis(30)).await;
            }
        });

        // Spawn an independent timer task.
        let ticker = spawn(async {
            sleep(Duration::from_millis(50)).await;
            println!("[ticker]  50 ms elapsed");
            sleep(Duration::from_millis(50)).await;
            println!("[ticker]  100 ms elapsed");
        });

        producer.await;
        consumer.await;
        ticker.await;
    });

    println!(
        "\nTotal wall-clock time: {:?}",
        start.elapsed()
    );
}
