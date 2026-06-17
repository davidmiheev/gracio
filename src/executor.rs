//! Core executor loop.
//!
//! `run_blocking` is the entry point.  It initialises thread-local state,
//! spawns the user's "main" future as task 0, and enters the poll loop.
//!
//! # Fair round-robin scheduling
//! Tasks are popped from the **front** of a `VecDeque` and re-pushed to the
//! **back** if they return `Pending`.  This guarantees every ready task gets
//! polled before any task gets a second turn.
//!
//! # CPU-efficient parking
//! When the ready queue is empty, the executor checks the timer wheel for the
//! next deadline and calls `std::thread::sleep` — **not** a spin loop.

use std::{
    task::Context,
    thread,
    time::Instant,
};

use crate::{sleep, waker, JoinHandle, READY_QUEUE, TASKS, TIMERS};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Run the executor, blocking the current thread until `future` completes.
///
/// While running, other futures can be spawned via [`crate::spawn`].
///
/// # Panics
/// Panics if a deadlock is detected (no ready tasks and no timers, but the
/// main future hasn't completed).
///
/// # Example
/// ```rust
/// use gracio::{run_blocking, spawn};
///
/// let result = run_blocking(async {
///     let a = spawn(async { 1 });
///     let b = spawn(async { 2 });
///     a.await + b.await
/// });
/// assert_eq!(result, 3);
/// ```
pub fn run_blocking<F>(future: F) -> F::Output
where
    F: std::future::Future + 'static,
    F::Output: 'static,
{
    // Spawn the "main" future as task 0.
    let main_handle = crate::spawn(future);

    loop {
        // ── Phase 1: poll ready tasks ──────────────────────────
        loop {
            let task_id = READY_QUEUE.with(|q| q.borrow_mut().pop_front());

            let Some(id) = task_id else { break };

            // Take the task out of the slab for the duration of this poll.
            // If it returns `Pending`, we put it back at the **same** slot
            // so the task keeps its id (and the waker's task_id stays valid).
            let task_opt = TASKS.with(|t| t.borrow_mut().remove(id));

            if let Some(mut entry) = task_opt {
                // Build a waker from the executor-owned `Rc<TaskWaker>`.
                let waker = waker::build_waker(&entry.waker);
                let mut cx = Context::from_waker(&waker);

                if entry.future.as_mut().poll(&mut cx).is_pending() {
                    // Re-insert at the SAME slot so the id is preserved.
                    TASKS.with(|t| t.borrow_mut().insert_at(id, entry));
                }
                // If Ready, `entry` drops here, releasing the `Rc<TaskWaker>`
                // and the `Box<dyn Future>`.
            }

            // Early exit: did the main future finish during this poll cycle?
            if try_return_main(&main_handle) {
                return unwrap_main(main_handle);
            }
        }

        // ── Phase 2: drain expired timers ──────────────────────
        let now = Instant::now();
        let next_deadline = sleep::drain_expired(now);

        // ── Phase 3: re-check ready queue (timers may have woken tasks) ──
        if !READY_QUEUE.with(|q| q.borrow().is_empty()) {
            continue;
        }

        // ── Phase 4: park or deadlock ──────────────────────────
        if try_return_main(&main_handle) {
            return unwrap_main(main_handle);
        }

        match next_deadline {
            Some(deadline) => {
                let delay = deadline.saturating_duration_since(Instant::now());
                thread::sleep(delay);
            }
            None => {
                panic!("Deadlock: no ready tasks, no timers, and the main future hasn't completed.");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns `true` if the main future's result is ready.
fn try_return_main<T>(handle: &JoinHandle<T>) -> bool {
    handle.shared.borrow().result.is_some()
}

/// Take the result out of the main `JoinHandle` and clean up thread-local state.
fn unwrap_main<T>(handle: JoinHandle<T>) -> T {
    let res = handle.shared.borrow_mut().result.take().unwrap();
    TASKS.with(|t| t.borrow_mut().clear());
    TIMERS.with(|t| t.borrow_mut().clear());
    res
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc, time::Instant};

    use crate::{bounded_channel, run_blocking, sleep, spawn};

    #[test]
    fn spawn_and_await() {
        let res = run_blocking(async {
            let h1 = spawn(async { 10 });
            let h2 = spawn(async { 20 });
            h1.await + h2.await
        });
        assert_eq!(res, 30);
    }

    #[test]
    fn sleep_suspends() {
        let start = Instant::now();
        run_blocking(async {
            sleep(std::time::Duration::from_millis(50)).await;
        });
        assert!(start.elapsed() >= std::time::Duration::from_millis(50));
    }

    #[test]
    fn channel_send_recv() {
        run_blocking(async {
            let (tx, rx) = bounded_channel::<i32>(2);
            spawn(async move {
                tx.send(1).await;
                tx.send(2).await;
                tx.send(3).await; // blocks until a recv frees space
            });

            assert_eq!(rx.recv().await, 1);
            assert_eq!(rx.recv().await, 2);
            assert_eq!(rx.recv().await, 3);
        });
    }

    #[test]
    fn round_robin_fairness() {
        let res = run_blocking(async {
            let log = Rc::new(RefCell::new(Vec::new()));

            let l1 = log.clone();
            spawn(async move {
                l1.borrow_mut().push("A1");
                sleep(std::time::Duration::from_millis(10)).await;
                l1.borrow_mut().push("A2");
            });

            let l2 = log.clone();
            spawn(async move {
                l2.borrow_mut().push("B1");
                sleep(std::time::Duration::from_millis(10)).await;
                l2.borrow_mut().push("B2");
            });

            // Let the spawned tasks run — main task yields
            sleep(std::time::Duration::from_millis(30)).await;

            let snapshot = log.borrow().clone();
            snapshot
        });

        assert_eq!(&res[0..2], &["A1", "B1"]);
        assert_eq!(&res[2..4], &["A2", "B2"]);
    }

    #[test]
    fn channel_backpressure() {
        run_blocking(async {
            let (tx, rx) = bounded_channel::<String>(1);
            let log = Rc::new(RefCell::new(Vec::new()));

            let l = log.clone();
            spawn(async move {
                l.borrow_mut().push("send-1");
                tx.send("a".into()).await;
                l.borrow_mut().push("send-2");
                tx.send("b".into()).await; // blocks
                l.borrow_mut().push("send-3");
                tx.send("c".into()).await; // blocks
            });

            sleep(std::time::Duration::from_millis(5)).await;
            assert_eq!(rx.recv().await, "a");
            assert_eq!(rx.recv().await, "b");
            assert_eq!(rx.recv().await, "c");

            assert_eq!(&*log.borrow(), &["send-1", "send-2", "send-3"]);
        });
    }

    #[test]
    fn try_recv_nonblocking() {
        run_blocking(async {
            let (tx, rx) = bounded_channel::<i32>(2);
            assert!(rx.try_recv().is_none());
            spawn(async move {
                tx.send(5).await;
            });
            sleep(std::time::Duration::from_millis(5)).await;
            assert_eq!(rx.try_recv(), Some(5));
            assert_eq!(rx.try_recv(), None);
        });
    }

    #[test]
    fn nested_spawn() {
        let res = run_blocking(async {
            spawn(async {
                spawn(async {
                    // deeply nested
                });
            });
            42
        });
        assert_eq!(res, 42);
    }

    #[test]
    fn multiple_sleeps() {
        let start = Instant::now();
        run_blocking(async {
            sleep(std::time::Duration::from_millis(10)).await;
            sleep(std::time::Duration::from_millis(10)).await;
            sleep(std::time::Duration::from_millis(10)).await;
        });
        assert!(start.elapsed() >= std::time::Duration::from_millis(30));
    }

    #[test]
    fn spawn_many() {
        run_blocking(async {
            let mut handles = Vec::new();
            for i in 0..50 {
                handles.push(spawn(async move { i }));
            }
            let mut sum = 0;
            for h in handles {
                sum += h.await;
            }
            // Sum 0..50 = 1225
            assert_eq!(sum, 1225);
        });
    }
}
