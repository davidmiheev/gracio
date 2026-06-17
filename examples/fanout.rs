//! Fan-out: one source, many workers, each receiving the full stream.
//!
//! A producer emits `1..=N` and **fans out** each value into K independent
//! channels, one per worker.  Every worker therefore sees the complete
//! stream and processes it independently — the canonical fan-out
//! pattern from reactive programming and actor frameworks.
//!
//! Run with:
//! ```bash
//! cargo run --example fanout
//! ```

use gracio::{bounded_channel, run_blocking, sleep, spawn, Receiver};
use std::time::Duration;

fn main() {
    run_blocking(async {
        const N_VALUES: i64 = 8; // values emitted by the producer
        const N_WORKERS: usize = 4; // fan-out width

        // Build one (tx, rx) pair per worker.  Each worker gets its own
        // receiver, so values are delivered to *every* worker in lock-step.
        let mut worker_txs = Vec::with_capacity(N_WORKERS);
        let mut worker_rxs: Vec<Receiver<i64>> = Vec::with_capacity(N_WORKERS);
        for _ in 0..N_WORKERS {
            let (tx, rx) = bounded_channel::<i64>(N_VALUES as usize);
            worker_txs.push(tx);
            worker_rxs.push(rx);
        }

        // Producer: emit 1..=N_VALUES, fanning each value out to every
        // worker in order.  Backpressure: send() blocks if a worker is
        // slow, so the producer naturally paces itself.
        let producer = spawn(async move {
            for i in 1..=N_VALUES {
                println!("[producer] broadcasting {i}");
                for tx in &worker_txs {
                    tx.send(i).await;
                }
            }
        });

        // Spawn workers — each one drains its own receiver.
        let mut workers = Vec::new();
        for (worker_id, rx) in worker_rxs.into_iter().enumerate() {
            workers.push(spawn(async move {
                let mut sum = 0i64;
                for _ in 0..N_VALUES {
                    let v = rx.recv().await;
                    sum += v * v;
                    println!("  [worker {worker_id}] received {v}, partial = {sum}");
                    sleep(Duration::from_millis(15)).await;
                }
                sum
            }));
        }

        producer.await;

        // Wait for all workers and collect their partial sums.
        let mut total = 0i64;
        for w in workers {
            total += w.await;
        }

        // Every worker sums 1² + 2² + … + N² = N(N+1)(2N+1)/6.
        // With N_WORKERS workers, total = N_WORKERS × that sum.
        let n = N_VALUES;
        let per_worker = n * (n + 1) * (2 * n + 1) / 6;
        let expected = per_worker * N_WORKERS as i64;

        println!("\nfan-out total: {total}");
        println!("expected ({} × Σ i²): {expected}", N_WORKERS);
        assert_eq!(total, expected);
    });
}
