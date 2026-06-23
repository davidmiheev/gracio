# Gracio 🦀

**A custom single-threaded async runtime built from scratch — demystifying how async Rust works under the hood.**

[![Rust](https://img.shields.io/badge/rust-stable-orange.svg)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-MIT.svg)](LICENSE)
[![dependencies](https://img.shields.io/badge/deps-0-brightgreen.svg)]()

Gracio implements a complete async runtime — executor, wakers, timers, and channels — using **only Rust's standard library**. No `tokio`, no `async-std`, no external crates.

---

## ✨ Features

- ⚡ **Fair round-robin scheduling** — every task gets polled, no starvation
- 🧵 **True single-threaded** — `Rc<RefCell<>>` everywhere, **zero atomics, zero locks**
- 🛠️ **Hand-rolled `RawWaker` + `RawWakerVTable`** — no `std::task::Wake` boilerplate
- ⏱️ **`sleep`** — timer-based suspension with `std::thread::sleep` CPU efficiency
- 📡 **`bounded_channel`** — async MPSC with backpressure and FIFO waker fairness
- 🔗 **`spawn` + `JoinHandle`** — fork-join concurrency without passing an executor around
- 🗄️ **Slab-allocated tasks** — dense vector-backed task storage, no `HashMap` hashing
- 🧪 **Rich test suite** — 21 tests covering fairness, backpressure, nested spawn, the slab itself, and more
- 🎓 **Seven runnable examples** — demo, fan-out, pipeline, ping-pong, worker pool, recursive spawn, timeout
- 📦 **Zero dependencies** — compiles in seconds

---

## 🚀 Quick Start

```bash
git clone https://github.com/user/gracio.git
cd gracio
cargo test          # run all tests
cargo run --example demo  # see it in action
```

Add to your `Cargo.toml`:

```toml
[dependencies]
gracio = "0.1.0"
```

---

## 📖 Usage

### `run_blocking` + `spawn`

```rust
use gracio::{run_blocking, spawn};

run_blocking(async {
    let a = spawn(async { 42 });
    let b = spawn(async { 10 });
    println!("{}", a.await + b.await); // 52
});
```

### `sleep`

```rust
use gracio::{run_blocking, sleep};
use std::time::Duration;

run_blocking(async {
    sleep(Duration::from_millis(100)).await;
    println!("100 ms later…");
});
```

### `bounded_channel`

```rust
use gracio::{run_blocking, spawn, bounded_channel};

run_blocking(async {
    let (tx, rx) = bounded_channel(2); // capacity 2

    spawn(async move {
        tx.send("hello").await;
        tx.send("world").await;
        tx.send("!").await; // blocks until a recv frees space
    });

    assert_eq!(rx.recv().await, "hello");
    assert_eq!(rx.recv().await, "world");
    assert_eq!(rx.recv().await, "!");
});
```

---

## 🧠 How It Works

### Architecture

```
┌──────────────────────────────────────────────────┐
│                    run_blocking()                 │
│                                                  │
│  ┌──────────┐   poll    ┌───────────────────┐    │
│  │ Ready    │ ────────► │  Slab<id,         │    │
│  │ Queue    │           │     TaskEntry{    │    │
│  │ (FIFO)   │ ◄──────── │       future,     │    │
│  │ Rc<Ref>  │  re-push  │       waker       │    │
│  └──────────┘           │     }             │    │
│       ▲                 └───────────────────┘    │
│       │                    │                     │
│       │ wake()       Poll::Pending               │
│       │                    ▼                     │
│  ┌────┴───────┐    ┌──────────────┐             │
│  │ TaskWaker  │    │ Register     │             │
│  │ (RawWaker) │    │ Waker in     │             │
│  │ Rc-based   │    │ Timer/Chan   │             │
│  └────────────┘    └──────────────┘             │
│                                                  │
│  ┌──────────────────────────────────────────┐    │
│  │ Timer Wheel (BTreeMap<Instant, Vec<Waker>>)│   │
│  │ → drains expired timers before sleeping  │    │
│  └──────────────────────────────────────────┘    │
└──────────────────────────────────────────────────┘
```

1. **`run_blocking`** spawns the user's future as task 0, then enters the poll loop.
2. Each task is polled. If it returns `Poll::Pending`, its `TaskEntry` is re-inserted into the slab at the **same** id (via `Slab::insert_at`) and its `Waker` is registered (in a timer or channel).
3. When a timer expires or a channel unblocks, `wake()` pushes the task id back onto the ready queue.
4. If the ready queue is empty, the executor sleeps until the next timer deadline — **zero CPU spin**.
5. Tasks are polled **FIFO** (round-robin), guaranteeing fairness.

### Task storage: a hand-rolled `Slab<T>`

`src/slab.rs` implements a `Vec<Option<T>>`-backed key/value store. Each
spawned task gets a monotonically increasing id from `NEXT_ID`, and that
id is used as the slab key via `Slab::insert_at`. Compared to a `HashMap`
this gives us:

| Property           | `HashMap<usize, _>` | `Slab<TaskEntry>` |
|--------------------|---------------------|---------------------|
| Lookup             | hash + probe        | single bounds check |
| Cache behaviour    | scattered buckets   | contiguous `Vec`    |
| Insert at known id | always allowed      | allowed (pad with tombstones) |
| Remove cost        | O(1)                | O(1)               |
| Iteration          | walks hash table    | walks a `Vec`      |

Removed slots become tombstones — the vector never shrinks, so old ids
cannot accidentally resolve to a different task after a slot is reused.
This is the same pattern used by `tokio::task::JoinSet` and `mio`'s
token tables, just hand-rolled so we keep **zero external dependencies**.

### Why no `Arc<Mutex<>>`?

Because Gracio is strictly single-threaded, we use `Rc<RefCell<>>` everywhere. This eliminates:

- Atomic reference-counting overhead
- Mutex lock/unlock contention
- `Send + Sync` trait noise

This is the single biggest performance differentiator vs. general-purpose runtimes.

The standard `std::task::Wake` trait takes `Arc<Self>`, which forces `Arc<Mutex<>>` for any shared mutable state. Gracio instead hand-rolls a `RawWaker` with an `Rc<TaskWaker>` data pointer, bypassing that constraint. See [`src/waker.rs`](src/waker.rs) for the full implementation.

The `Rc` + `RawWakerVTable` dance is the **only** `unsafe` in the entire runtime — and it's a 30-line, well-contained module. The rest of the crate is 100% safe Rust.

```rust
// src/waker.rs (excerpt) — a complete RawWaker vtable
unsafe fn waker_clone(data: *const ()) -> RawWaker {
    unsafe { Rc::increment_strong_count(data as *const TaskWaker) };
    RawWaker::new(data, &VTABLE)
}
unsafe fn waker_wake(data: *const ()) {
    let rc = unsafe { Rc::from_raw(data as *const TaskWaker) };
    READY_QUEUE.with(|q| q.borrow_mut().push_back(rc.task_id));
}
unsafe fn waker_wake_by_ref(data: *const ()) {
    let task_id = unsafe { (*(data as *const TaskWaker)).task_id };
    READY_QUEUE.with(|q| q.borrow_mut().push_back(task_id));
}
unsafe fn waker_drop(data: *const ()) {
    let _ = unsafe { Rc::from_raw(data as *const TaskWaker) };
}
```

---

## 🧵 Project Structure

```
src/
├── lib.rs          # Public API, thread-local state, spawn()
├── executor.rs     # Main poll loop, deadlock detection
├── slab.rs         # Slab<T> dense task storage
├── waker.rs        # TaskWaker (Wake trait impl)
├── join_handle.rs  # JoinHandle<T> future + spawn internals
├── sleep.rs        # sleep(), drain_expired()
└── channel.rs      # bounded_channel(), Sender, Receiver
examples/
├── demo.rs              # Producer + consumer + ticker
├── fanout.rs            # One source → K workers via N channels
├── pipeline.rs          # Three-stage CPU-style pipeline
├── ping_pong.rs         # Two tasks trading values over two channels
├── worker_pool.rs       # K workers consuming from a job queue
├── recursive_spawn.rs   # Tree of spawned tasks (binary, depth D)
└── timeout.rs           # Race sleep() against a real operation
```

| Module | Responsibility |
|--------|---------------|
| `lib.rs` | Public re-exports, `spawn()`, thread-local globals |
| `executor.rs` | `run_blocking()` poll loop, deadlock detection |
| `slab.rs` | `Slab<T>` — dense vector-backed task storage with `insert_at` |
| `waker.rs` | Hand-rolled `RawWaker` + vtable using `Rc<TaskWaker>` |
| `join_handle.rs` | `JoinHandle<T>` future + spawn internals |
| `sleep.rs` | `sleep()` future + `drain_expired()` for timers |
| `channel.rs` | `bounded_channel()` — `Sender`, `Receiver`, backpressure |

---

## 🎓 Examples

All examples are runnable with `cargo run --example <name>`.

| Example | What it shows |
|---------|---------------|
| [`demo`](examples/demo.rs) | Producer + consumer + timer ticker — the canonical hello-world of async runtimes |
| [`fanout`](examples/fanout.rs) | One source broadcasting `1..=N` to **K independent workers**, each receiving the full stream |
| [`pipeline`](examples/pipeline.rs) | Three-stage transformation pipeline (×2, +100, sink) demonstrating stage-to-stage backpressure |
| [`ping_pong`](examples/ping_pong.rs) | Two tasks trading `"ping"` / `"pong"` over a pair of channels — no shared state at all |
| [`worker_pool`](examples/worker_pool.rs) | K long-lived workers each with their own inbox; a producer dispatches jobs round-robin |
| [`recursive_spawn`](examples/recursive_spawn.rs) | A binary tree of spawned tasks where each internal node awaits its two children |
| [`timeout`](examples/timeout.rs) | The "race `sleep` against the work" timeout pattern — what `select!` would give us for free |

---

## ⚡ Performance Considerations

| Technique | Benefit |
|-----------|---------|
| `Rc<RefCell<>>` over `Arc<Mutex<>>` | No atomic ops, no lock contention |
| Hand-rolled `RawWaker` vtable | Avoids `Arc` requirement of `std::task::Wake` |
| `VecDeque` FIFO polling | O(1) push/pop, inherent round-robin |
| `BTreeMap::split_off` for timer drain | O(log n) bulk-expiry instead of iterating |
| `thread::sleep` on empty queue | Parks the thread exactly until next deadline |
| `wrapping_add` for task ids | Avoids overflow panics on long-lived executors |
| `try_recv` non-blocking channel read | Allows polling without suspending |
| `Slab<TaskEntry>` over `HashMap<usize, _>` | Bounds-checked index lookup, contiguous memory, no hashing |

### Potential future improvements

- [ ] **Cooperative budgeting** — limit polls per task per tick
- [ ] **`select!` macro** — race multiple futures
- [ ] **`LocalWaker` / wake-by-ref** — avoid `Rc` clone on every wake
- [x] **Slab allocation** — dense task storage instead of `HashMap`
- [ ] **`Sender::close()` + `recv() → Option<T>`** — let workers drain channels naturally
- [ ] **`JoinSet`** — collect multiple JoinHandles

---

## 📜 License

Licensed under either of [MIT](LICENSE-MIT)

---

*Built for learning. Not for production (yet).*
