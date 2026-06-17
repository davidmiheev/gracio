//! Timeout: race a `sleep` against a real operation using channels.
//!
//! Real async runtimes have a `select!` macro that races multiple
//! futures.  We don't have one (yet — see the README), but the same idea
//! can be expressed with a small "done" channel: spawn the work, then
//! `sleep` for the deadline, and finally check whether the work
//! completed.  If not, the operation timed out.
//!
//! This is widely used in production code: tokio's `timeout()`,
//! async-std's `timeout()`, and many services rely on the same
//! "race the timer against the work" idea.
//!
//! Run with:
//! ```bash
//! cargo run --example timeout
//! ```

use gracio::{bounded_channel, run_blocking, sleep, spawn};
use std::time::{Duration, Instant};

async fn fetch_user(id: u32) -> String {
    // Simulate a network call.  Try changing `delay` to 100ms to see
    // the timeout trigger instead of completing.
    let delay = 20u64;
    sleep(Duration::from_millis(delay)).await;
    format!("user#{id}")
}

fn main() {
    run_blocking(async {
        // Quick fetch: finishes well within the timeout.
        let start = Instant::now();
        let quick = with_timeout("quick-fetch", Duration::from_millis(100), fetch_user(7)).await;
        println!(
            "quick-fetch → {:?} (took {:?})",
            quick,
            start.elapsed()
        );
        assert_eq!(quick.as_deref(), Some("user#7"));

        // Slow fetch: exceeds the 5 ms timeout.
        let start2 = Instant::now();
        let slow = with_timeout(
            "too-slow-fetch",
            Duration::from_millis(5),
            async {
                sleep(Duration::from_millis(50)).await;
                "user#999".to_string()
            },
        )
        .await;
        println!(
            "too-slow-fetch → {:?} (took {:?})",
            slow,
            start2.elapsed()
        );
        assert!(slow.is_none(), "slow fetch should time out");

        println!("\n✓ timeout pattern works as expected");
    });
}

/// Run `fut` but give up after `timeout`.  Returns `None` on timeout,
/// `Some(value)` if it finished in time.
///
/// Implementation: spawn the work, which signals on a "done" channel
/// when complete.  Then `sleep(timeout)` and check whether the work
/// already finished.  If yes, await the JoinHandle for the value; if
/// no, give up and return `None`.
async fn with_timeout<F>(label: &str, timeout: Duration, fut: F) -> Option<String>
where
    F: std::future::Future<Output = String> + 'static,
{
    let (done_tx, done_rx) = bounded_channel::<String>(1);

    // Worker: runs the future and ships the result on the done channel.
    let handle = spawn(async move {
        let result = fut.await;
        // Try to send — capacity 1 has room for one send before any recv.
        let _ = done_tx.send(result).await;
    });

    // Sleep the deadline, then check if the work is already done.
    sleep(timeout).await;

    match done_rx.try_recv() {
        Some(value) => {
            // The work finished in time — make sure the JoinHandle is
            // also awaited so its task slot is freed cleanly.
            handle.await;
            println!("  [{label}] completed within {timeout:?}");
            Some(value)
        }
        None => {
            println!("  [{label}] timed out after {timeout:?}");
            // Leave the worker task running — it will eventually
            // complete and signal the done channel.  Since our runtime
            // exits when the main future returns, any orphan workers are
            // cleaned up automatically by `unwrap_main`.
            None
        }
    }
}
