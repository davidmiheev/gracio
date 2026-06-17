//! Worker pool: a producer feeds N jobs to a pool of K workers.
//!
//! Each worker is a long-lived task that pulls from its private inbox
//! channel.  A producer dispatches N jobs round-robin across the
//! workers; each worker processes its share and emits results on a
//! shared result channel, which a collector drains.
//!
//! Because our channel has no built-in close mechanism, each worker
//! knows exactly how many jobs to expect and exits cleanly when its
//! share is done.  A more polished runtime would add `Sender::close()`
//! so workers can `recv()` until None — see the README.
//!
//! Run with:
//! ```bash
//! cargo run --example worker_pool
//! ```

use gracio::{bounded_channel, run_blocking, sleep, spawn, Receiver, Sender};
use std::time::Duration;

#[derive(Debug)]
struct Job {
    id: usize,
    payload: i32,
}

#[derive(Debug)]
struct Result_ {
    job_id: usize,
    output: i32,
    worker_id: usize,
}

fn main() {
    run_blocking(async {
        const N_JOBS: usize = 12;
        const N_WORKERS: usize = 3;

        // One inbox per worker.  The producer decides which inbox to
        // push each job into.  Workers read from their own inbox only.
        let mut worker_inboxes: Vec<Sender<Job>> = Vec::with_capacity(N_WORKERS);
        let mut worker_outboxes: Vec<Receiver<Job>> = Vec::with_capacity(N_WORKERS);
        for _ in 0..N_WORKERS {
            let (tx, rx) = bounded_channel::<Job>(1);
            worker_inboxes.push(tx);
            worker_outboxes.push(rx);
        }

        // Shared result channel: every worker writes here, the collector reads.
        let (res_tx, res_rx) = bounded_channel::<Result_>(4);

        // Each worker gets exactly N_JOBS / N_WORKERS jobs.
        let jobs_per_worker = N_JOBS / N_WORKERS;

        // Spawn workers.
        let mut workers = Vec::new();
        for (worker_id, inbox) in worker_outboxes.into_iter().enumerate() {
            let res_tx = res_tx.clone();
            workers.push(spawn(async move {
                for _ in 0..jobs_per_worker {
                    let job = inbox.recv().await;
                    // Simulate work — payload-dependent duration.
                    sleep(Duration::from_millis(15 * (1 + job.payload as u64 % 3))).await;
                    let output = job.payload.pow(2) + job.payload;
                    println!(
                        "  [worker {worker_id}] job {} → output {output}",
                        job.id
                    );
                    res_tx
                        .send(Result_ {
                            job_id: job.id,
                            output,
                            worker_id,
                        })
                        .await;
                }
                println!("  [worker {worker_id}] share complete, exiting");
            }));
        }

        // Producer: emits N_JOBS jobs round-robin to the worker inboxes.
        let producer = spawn(async move {
            for (i, inbox) in worker_inboxes.into_iter().cycle().take(N_JOBS).enumerate() {
                inbox
                    .send(Job {
                        id: i,
                        payload: i as i32,
                    })
                    .await;
            }
            println!("[producer] all jobs dispatched");
        });

        // Collector: receive exactly N_JOBS results.
        let collector = spawn(async move {
            let mut results = Vec::with_capacity(N_JOBS);
            for _ in 0..N_JOBS {
                let r = res_rx.recv().await;
                results.push(r);
            }
            results
        });

        producer.await;
        let results = collector.await;

        // Wait for all workers to drain their inboxes and exit.
        for w in workers {
            w.await;
        }

        println!("\ncollected {} results:", results.len());
        for r in &results {
            println!(
                "  job {:>2} → output {:>3} (worker {})",
                r.job_id, r.output, r.worker_id
            );
        }

        // Every job should appear exactly once.
        let mut ids: Vec<usize> = results.iter().map(|r| r.job_id).collect();
        ids.sort();
        assert_eq!(ids, (0..N_JOBS).collect::<Vec<_>>());
        // Every job i's output should be i² + i.
        for r in &results {
            let expected = (r.job_id as i32).pow(2) + r.job_id as i32;
            assert_eq!(r.output, expected);
        }
        println!("✓ all jobs processed exactly once with correct output");
    });
}
