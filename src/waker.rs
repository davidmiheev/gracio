//! Waker implementation built on `RawWaker` + `RawWakerVTable`.
//!
//! We use `Rc<TaskWaker>` as the data pointer of a `RawWaker`.  The vtable's
//! `clone` function calls `Rc::clone` — no atomics.  The `wake` function
//! consumes the `Rc` and pushes the task id onto the ready queue, then drops
//! it.  `wake_by_ref` does the same without consuming.
//!
//! This avoids `Arc<Mutex<>>` entirely: no atomic refcounting, no lock
//! acquisition.  The only `unsafe` is the vtable construction itself, which
//! is encapsulated here so the rest of the runtime can stay safe.
//!
//! Safety notes:
//! - The data pointer always points to a live `Rc<TaskWaker>` whose strong
//!   count is at least 1 (the one we're handing out, plus any clones).
//! - The vtable's `clone`/`wake`/`wake_by_ref`/`drop` functions are only ever
//!   invoked through `Waker::clone` / `Waker::wake` / `Waker::drop`, which the
//!   standard library guarantees will pass the same `*const ()` we set.

use std::{
    rc::Rc,
    task::{RawWaker, RawWakerVTable, Waker},
};

use crate::READY_QUEUE;

// ---------------------------------------------------------------------------
// Data
// ---------------------------------------------------------------------------

/// Waker payload.  The `task_id` is pushed onto the global ready queue when
/// `wake` is called.
pub(crate) struct TaskWaker {
    pub task_id: usize,
}

// ---------------------------------------------------------------------------
// VTable functions
// ---------------------------------------------------------------------------

/// Bump the strong count of the `Rc<TaskWaker>` and return a `RawWaker` that
/// owns one of the new references.
unsafe fn waker_clone(data: *const ()) -> RawWaker {
    unsafe { Rc::increment_strong_count(data as *const TaskWaker) };
    RawWaker::new(data, &VTABLE)
}

/// Consume the `Rc<TaskWaker>`, push the task id to the ready queue, then
/// drop the `Rc` (decrementing its strong count).
unsafe fn waker_wake(data: *const ()) {
    // Reconstruct the Rc *without* incrementing (we own the incoming strong
    // reference) so the eventual `drop` correctly decrements it to zero.
    let rc = unsafe { Rc::from_raw(data as *const TaskWaker) };
    READY_QUEUE.with(|q| q.borrow_mut().push_back(rc.task_id));
    // `rc` drops here, releasing the strong reference.
}

/// Same as `waker_wake` but the caller retains its reference.  We do not
/// construct a new `Rc` (so we don't disturb the strong count) — we just
/// read the `task_id` field through the raw pointer and push it.
unsafe fn waker_wake_by_ref(data: *const ()) {
    // SAFETY: `data` points to a live `Rc<TaskWaker>` allocation, which is
    // always properly aligned and valid for reads as long as some `Rc`
    // points to it (which it must, since we were just called).
    let task_id = unsafe { (*(data as *const TaskWaker)).task_id };
    READY_QUEUE.with(|q| q.borrow_mut().push_back(task_id));
}

/// Drop the `Rc<TaskWaker>` (decrement strong count, free if it hits 0).
unsafe fn waker_drop(data: *const ()) {
    let _ = unsafe { Rc::from_raw(data as *const TaskWaker) };
}

static VTABLE: RawWakerVTable = RawWakerVTable::new(
    waker_clone,
    waker_wake,
    waker_wake_by_ref,
    waker_drop,
);

// ---------------------------------------------------------------------------
// Public-ish constructor
// ---------------------------------------------------------------------------

/// Build a `Waker` for the given `Rc<TaskWaker>`.  The `Waker` owns one
/// strong reference; when it's dropped, the reference is released.
pub(crate) fn build_waker(data: &Rc<TaskWaker>) -> Waker {
    // Bump the refcount so the `Waker` owns one strong reference.
    let data_ptr = Rc::into_raw(Rc::clone(data));
    unsafe { Waker::from_raw(RawWaker::new(data_ptr.cast::<()>(), &VTABLE)) }
}

/// Allocate the `Rc<TaskWaker>` used as the waker's data.  The executor keeps
/// one `Rc` and hands clones out to whoever needs to wake the task.
pub(crate) fn make_waker_data(task_id: usize) -> Rc<TaskWaker> {
    Rc::new(TaskWaker { task_id })
}
