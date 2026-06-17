//! Pipeline: a chain of stages connected by channels.
//!
//! producer → [stage A] → channel → [stage B] → channel → [stage C] → sink
//!
//! Each stage processes one value at a time, transforming it before
//! passing it down the line.  Stages run concurrently: while stage A is
//! working on value N, stage B is already processing N-1, and stage C
//! is wrapping up N-2.  This is the classic "pipelined CPU" pattern.
//!
//! Because our channel has no built-in close mechanism, each stage
//! knows how many values to expect and exits cleanly when its count is
//! reached.  A more polished runtime would add `Sender::close()` and
//! let `recv()` return `None` once all senders are dropped — that's a
//! future improvement listed in the README.
//!
//! Run with:
//! ```bash
//! cargo run --example pipeline
//! ```

use gracio::{bounded_channel, run_blocking, sleep, spawn};
use std::time::Duration;

const N: i32 = 6; // values flowing through the pipeline

fn main() {
    run_blocking(async {
        // Build the three inter-stage channels.  Capacity 1 maximises
        // pipelining: each stage blocks on `send()` as soon as the next
        // stage is busy, so values flow at the speed of the slowest
        // stage without queueing up.
        let (a_tx, a_rx) = bounded_channel::<i32>(1);
        let (b_tx, b_rx) = bounded_channel::<i32>(1);
        let (c_tx, c_rx) = bounded_channel::<i32>(1);

        // Producer: emits 1..=N.
        spawn(async move {
            for i in 1..=N {
                println!("[producer] → {i}");
                a_tx.send(i).await;
            }
        });

        // Stage A: doubles every value, then forwards.
        spawn(async move {
            for _ in 0..N {
                let v = a_rx.recv().await;
                let out = v * 2;
                println!("[A]      {v:>3} → {out}");
                sleep(Duration::from_millis(20)).await;
                b_tx.send(out).await;
            }
        });

        // Stage B: adds 100.
        spawn(async move {
            for _ in 0..N {
                let v = b_rx.recv().await;
                let out = v + 100;
                println!("[B]      {v:>3} → {out}");
                sleep(Duration::from_millis(20)).await;
                c_tx.send(out).await;
            }
        });

        // Sink: collects every value.
        let sink = spawn(async move {
            let mut out = Vec::with_capacity(N as usize);
            for _ in 0..N {
                let v = c_rx.recv().await;
                println!("[sink]       ← {v}");
                out.push(v);
            }
            out
        });

        let results = sink.await;
        println!("\npipeline output: {:?}", results);

        // Every input i should come out as 2*i + 100.
        let expected: Vec<i32> = (1..=N).map(|i| 2 * i + 100).collect();
        assert_eq!(results, expected);
        println!("✓ all values match expected transformation (2x + 100)");
    });
}
