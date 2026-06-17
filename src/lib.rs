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
mod slab;
mod sleep;
mod waker;

pub use channel::{bounded_channel, Receiver, Sender};
pub use executor::run_blocking;
pub use join_handle::JoinHandle;
pub use sleep::sleep;

use std::{
    cell::RefCell,
    collections::{BTreeMap, VecDeque},
    pin::Pin,
    rc::Rc,
    task::Waker,
};

use crate::slab::Slab;

// ---------------------------------------------------------------------------
// Thread-local runtime state
//
// All state is thread-local because this is a **single-threaded** executor.
// We use `Rc<RefCell<>>` everywhere — no atomics, no locks.
// ---------------------------------------------------------------------------

pub(crate) type BoxFuture<'a, T = ()> = Pin<Box<dyn std::future::Future<Output = T> + 'a>>;

/// One live task: the pinned future plus the executor-owned `Rc<TaskWaker>`
/// that keeps it alive across polls.
///
/// We store both in a single `Slab` entry so the executor can look up *all*
/// per-task state with one indexed load — no second hashmap lookup, no risk
/// of the two slabs disagreeing on which slot a task lives in.
pub(crate) struct TaskEntry {
    pub future: BoxFuture<'static>,
    pub waker: Rc<waker::TaskWaker>,
}

thread_local! {
    /// Slab of live tasks, indexed by task id.  Slot `id` is removed while a
    /// task is being polled (the executor owns the entry for that brief
    /// window) and re-inserted at the *same* slot if the task yields with
    /// `Pending`.
    ///
    /// We use a `Slab` rather than a `HashMap` because task ids are dense and
    /// sequential: lookups are a single bounds-checked index, with no hashing
    /// and no rehash pauses, and the data sits in a contiguous `Vec` for
    /// excellent cache behaviour.  See [`crate::slab`] for details.
    pub(crate) static TASKS: RefCell<Slab<TaskEntry>> =
        const { RefCell::new(Slab::new()) };

    /// Timer wheel: deadline → list of wakers to fire when the deadline passes.
    pub(crate) static TIMERS: RefCell<BTreeMap<std::time::Instant, Vec<Waker>>> =
        const { RefCell::new(BTreeMap::new()) };

    /// FIFO queue of task ids ready to be polled.
    pub(crate) static READY_QUEUE: RefCell<VecDeque<usize>> = const { RefCell::new(VecDeque::new()) };

    /// Monotonically increasing task-id counter.  Task ids are allocated by
    /// `fetch_add`-style increments here and used as slab keys via
    /// `Slab::insert_at`, so a task keeps the **same** id across remove /
    /// re-insert cycles during polling.  `wrapping_add` avoids panics on
    /// extremely long-lived executors.
    pub(crate) static NEXT_ID: RefCell<usize> = const { RefCell::new(0) };
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
