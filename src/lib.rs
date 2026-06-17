//! Gracio — A custom single-threaded async runtime built from scratch.
//!
//! Gracio demystifies how async Rust works under the hood by implementing
//! a complete executor, wakers, timers, and channels using only `std`.
//!
//! # Features
//! - **Fair round-robin scheduling** — no task starvation
//! - **`spawn` / `JoinHandle`** — fork-join concurrency
//! - **`sleep`** — timer-based suspension with zero CPU spin
//! - **`bounded_channel`** — async MPSC with backpressure
//! - **Zero external dependencies**
//!
//! # Example
//! ```rust
//! use gracio::{run_blocking, spawn, sleep, bounded_channel};
//! use std::time::Duration;
//!
//! run_blocking(async {
//!     let h1 = spawn(async { 42 });
//!     let h2 = spawn(async { sleep(Duration::from_millis(10)).await; 10 });
//!     assert_eq!(h1.await + h2.await, 52);
//! });
//! ```

mod channel;
mod executor;
mod join_handle;
mod sleep;
mod waker;

pub use channel::{bounded_channel, Receiver, Sender};
pub use executor::run_blocking;
pub use join_handle::JoinHandle;
pub use sleep::sleep;

use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap, VecDeque},
    pin::Pin,
    rc::Rc,
    task::Waker,
};

// ---------------------------------------------------------------------------
// Thread-local runtime state
//
// All state is thread-local because this is a **single-threaded** executor.
// We use `Rc<RefCell<>>` everywhere — no atomics, no locks.
// ---------------------------------------------------------------------------

pub(crate) type BoxFuture<'a, T = ()> = Pin<Box<dyn std::future::Future<Output = T> + 'a>>;

thread_local! {
    /// Map from task id → pinned future.  Removed while polling, re-inserted if Pending.
    pub(crate) static TASKS: RefCell<HashMap<usize, BoxFuture<'static>>> = RefCell::new(HashMap::new());

    /// Map from task id → the `Rc<TaskWaker>` the executor holds.  We keep one
    /// strong reference per task; the `Waker`s handed to futures are clones of
    /// this.  When the task completes, this `Rc` is dropped.
    pub(crate) static WAKERS: RefCell<HashMap<usize, Rc<waker::TaskWaker>>> = RefCell::new(HashMap::new());

    /// Timer wheel: deadline → list of wakers to fire when the deadline passes.
    pub(crate) static TIMERS: RefCell<BTreeMap<std::time::Instant, Vec<Waker>>> =
        const { RefCell::new(BTreeMap::new()) };

    /// FIFO queue of task ids ready to be polled.
    pub(crate) static READY_QUEUE: RefCell<VecDeque<usize>> = const { RefCell::new(VecDeque::new()) };

    /// Monotonically increasing task-id counter.  Task 0 is reserved for the
    /// "main" future spawned implicitly by `run_blocking`.
    pub(crate) static NEXT_ID: RefCell<usize> = const { RefCell::new(1) };
}

// ---------------------------------------------------------------------------
// spawn  (global function — no executor handle needed)
// ---------------------------------------------------------------------------

/// Spawn a future onto the running executor.
///
/// # Panics
/// Panics if no executor is running (i.e. called outside of `run_blocking`).
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: std::future::Future + 'static,
    F::Output: 'static,
{
    join_handle::spawn_impl(future)
}
