//! `JoinHandle` — the handle returned by `spawn()`.
//!
//! Awaits the result of a spawned task.  The spawned future's output is captured
//! and stored in `JoinState`, and the waiting future is woken when the result
//! is ready.

use std::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

/// Shared state between the spawned task and the `JoinHandle` future.
pub(crate) struct JoinState<T> {
    pub result: Option<T>,
    pub waker: Option<Waker>,
}

/// A handle that can be `.await`-ed to retrieve the output of a spawned future.
pub struct JoinHandle<T> {
    pub(crate) shared: Rc<RefCell<JoinState<T>>>,
}

// ---------------------------------------------------------------------------
// Future impl
// ---------------------------------------------------------------------------

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.shared.borrow_mut();
        match state.result.take() {
            Some(res) => Poll::Ready(res),
            None => {
                state.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

// ---------------------------------------------------------------------------
// spawn implementation (called from lib.rs)
// ---------------------------------------------------------------------------

pub(crate) fn spawn_impl<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    let shared = Rc::new(RefCell::new(JoinState {
        result: None,
        waker: None,
    }));
    let shared_clone = shared.clone();

    // Wrap the user's future so we capture its output and wake the JoinHandle.
    let wrapped = async move {
        let res = future.await;
        let mut state = shared_clone.borrow_mut();
        state.result = Some(res);
        if let Some(w) = state.waker.take() {
            w.wake();
        }
    };

    crate::NEXT_ID.with(|next| {
        let id = *next.borrow();
        *next.borrow_mut() = id.wrapping_add(1);
        // Register the executor-owned `Rc<TaskWaker>` for this task.
        crate::WAKERS.with(|w| {
            w.borrow_mut().insert(id, crate::waker::make_waker_data(id));
        });
        crate::READY_QUEUE.with(|q| q.borrow_mut().push_back(id));
        crate::TASKS.with(|t| {
            t.borrow_mut().insert(id, Box::pin(wrapped));
        });
    });

    JoinHandle { shared }
}
