//! Bounded MPSC (multi-producer, single-consumer) channel.
//!
//! Supports backpressure: `send()` returns `Pending` when the buffer is full,
//! and `recv()` returns `Pending` when the buffer is empty.  Wakers are stored
//! in vectors and woken FIFO-style.

use std::{
    cell::RefCell,
    collections::VecDeque,
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Create a bounded MPSC channel with the given capacity.
///
/// `capacity` must be ≥ 1.
pub fn bounded_channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(capacity > 0, "Channel capacity must be at least 1");
    let shared = Rc::new(RefCell::new(ChannelState {
        buffer: VecDeque::with_capacity(capacity),
        capacity,
        recv_wakers: Vec::new(),
        send_wakers: Vec::new(),
    }));
    (Sender { shared: shared.clone() }, Receiver { shared })
}

// ---------------------------------------------------------------------------
// Internal state
// ---------------------------------------------------------------------------

struct ChannelState<T> {
    buffer: VecDeque<T>,
    capacity: usize,
    recv_wakers: Vec<Waker>,
    send_wakers: Vec<Waker>,
}

// ---------------------------------------------------------------------------
// Sender
// ---------------------------------------------------------------------------

/// The sending half of a bounded channel.
pub struct Sender<T> {
    shared: Rc<RefCell<ChannelState<T>>>,
}

impl<T> Sender<T> {
    /// Send a value asynchronously.  Suspends until there is room in the buffer.
    pub async fn send(&self, value: T) {
        SendFuture {
            shared: &self.shared,
            value: RefCell::new(Some(value)),
        }
        .await
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

struct SendFuture<'a, T> {
    shared: &'a Rc<RefCell<ChannelState<T>>>,
    /// Wrapped in `RefCell` so we can `take()` without needing `Pin` projection.
    value: RefCell<Option<T>>,
}

impl<'a, T> Future for SendFuture<'a, T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.shared.borrow_mut();
        if state.buffer.len() < state.capacity {
            let val = self.value.borrow_mut().take().unwrap();
            state.buffer.push_back(val);
            // Wake exactly one blocked receiver (FIFO fairness)
            if let Some(w) = state.recv_wakers.pop() {
                w.wake();
            }
            Poll::Ready(())
        } else {
            state.send_wakers.push(cx.waker().clone());
            Poll::Pending
        }
    }
}

// ---------------------------------------------------------------------------
// Receiver
// ---------------------------------------------------------------------------

/// The receiving half of a bounded channel.
pub struct Receiver<T> {
    shared: Rc<RefCell<ChannelState<T>>>,
}

impl<T> Receiver<T> {
    /// Receive a value asynchronously.  Suspends until a value is available.
    pub async fn recv(&self) -> T {
        RecvFuture { shared: &self.shared }.await
    }

    /// Try to receive a value without blocking.
    pub fn try_recv(&self) -> Option<T> {
        let mut state = self.shared.borrow_mut();
        let val = state.buffer.pop_front();
        if val.is_some() {
            if let Some(w) = state.send_wakers.pop() {
                w.wake();
            }
        }
        val
    }
}

struct RecvFuture<'a, T> {
    shared: &'a Rc<RefCell<ChannelState<T>>>,
}

impl<'a, T> Future for RecvFuture<'a, T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.shared.borrow_mut();
        if let Some(val) = state.buffer.pop_front() {
            // Wake exactly one blocked sender (FIFO fairness)
            if let Some(w) = state.send_wakers.pop() {
                w.wake();
            }
            Poll::Ready(val)
        } else {
            state.recv_wakers.push(cx.waker().clone());
            Poll::Pending
        }
    }
}
