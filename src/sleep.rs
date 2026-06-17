//! Timer-based suspension primitive.
//!
//! `sleep(duration)` returns a future that completes after at least `duration`
//! has elapsed.  Internally it registers a `Waker` in a global `BTreeMap`
//! keyed by deadline (`Instant`).  The executor drains expired timers before
//! going to sleep.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Returns a future that completes after at least `duration` has elapsed.
///
/// # Example
/// ```rust
/// use gracio::{run_blocking, sleep};
/// use std::time::{Duration, Instant};
///
/// run_blocking(async {
///     let start = Instant::now();
///     sleep(Duration::from_millis(100)).await;
///     assert!(start.elapsed() >= Duration::from_millis(100));
/// });
/// ```
pub fn sleep(duration: Duration) -> SleepFuture {
    SleepFuture {
        deadline: Instant::now() + duration,
    }
}

// ---------------------------------------------------------------------------
// Sleep future
// ---------------------------------------------------------------------------

pub struct SleepFuture {
    deadline: Instant,
}

impl Future for SleepFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if Instant::now() >= self.deadline {
            Poll::Ready(())
        } else {
            crate::TIMERS.with(|timers| {
                timers
                    .borrow_mut()
                    .entry(self.deadline)
                    .or_default()
                    .push(cx.waker().clone());
            });
            Poll::Pending
        }
    }
}

// ---------------------------------------------------------------------------
// Timer-drain helper (called by executor)
// ---------------------------------------------------------------------------

/// Wake every timer whose deadline ≤ `now`.
/// Returns the next upcoming deadline, if any.
pub(crate) fn drain_expired(now: Instant) -> Option<Instant> {
    crate::TIMERS.with(|timers| {
        let mut timers = timers.borrow_mut();

        // `split_off` is O(log n) and gives us all entries ≥ now.
        // Keys < now are the expired ones.
        let expired = {
            let split = timers.split_off(&now);
            std::mem::replace(&mut *timers, split)
        };

        for (_, wakers) in expired {
            for w in wakers {
                w.wake();
            }
        }

        timers.keys().next().copied()
    })
}
