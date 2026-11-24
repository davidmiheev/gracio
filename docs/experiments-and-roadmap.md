# Gracio: possible experiments

*Drafted 18 Nov 2026, extended 21 Nov 2026.*

## Project snapshot

Gracio is a single-threaded, zero-dependency async runtime of about 1,750 lines of Rust (last commit 23 Jun 2026), so its experiments test its own design choices and use Tokio and Rayon as reference points. I read "all 3" as Gracio, Tokio and Rayon. Source: [davidmiheev/gracio](https://github.com/davidmiheev/gracio).

Reading the source turned up six behaviours worth testing directly:

| Behaviour in the code | Where | Why it matters |
| --- | --- | --- |
| Blocked senders and receivers are woken with `Vec::pop`, which is LIFO, while the comments say FIFO | `channel.rs` | The newest waiter wakes first, so early waiters can starve |
| Expired timers are drained only when the ready queue is empty | `executor.rs` | Busy tasks can delay sleepers for as long as they keep the queue non-empty |
| Task ids only grow and removed slots stay as tombstones (per the README) | `lib.rs`, `slab.rs` | Memory follows the number of tasks ever spawned, not the number alive |
| A pending `sleep` pushes a new waker clone on every poll | `sleep.rs` | A task polled again before its deadline leaves duplicate wakers in the timer map |
| Idle time is `thread::sleep` and wakers are `Rc` based | `executor.rs`, `waker.rs` | No other thread can wake it, and one blocking call freezes every task |
| No channel close and no cancel when a `JoinHandle` is dropped | `channel.rs`, `join_handle.rs` | Workers must know their job count, and dropped handles keep their task running |

## Experiments per project

Expectations below are hypotheses from reading the code and the docs, not measurements. Each experiment is meant to confirm or break one of them.

### Gracio

| # | Experiment | Setup | Measure | Expect |
| --- | --- | --- | --- | --- |
| G1 | Wake order | 100 tasks blocked on `recv` of one channel, then 100 sends | Order tasks resume in, and spread of wait times | LIFO order. Switching to `VecDeque` and `pop_front` restores FIFO |
| G2 | Timer starvation | Two tasks ping-pong 1M messages while a third does `sleep(10ms)` | How late the sleeper wakes, in ms | Wakes only after the ping-pong ends. Draining timers every K polls fixes it |
| G3 | Spawn cost and memory | Spawn 1M, then 10M no-op tasks, awaiting each batch | Tasks per second and peak RSS (`/usr/bin/time -v`) | RSS keeps growing with total spawned. A free-list slab keeps it flat |
| G4 | Wake overhead | Two tasks trade 10M values over two channels | Nanoseconds per round trip, plus a `samply` profile | `Rc` clone and `RefCell` borrow dominate. Wake-by-ref (a README TODO) trims it |
| G5 | Timer scale | 100k tasks each sleep a random 1 to 100 ms | Lateness per timer, polls per task, `BTreeMap` size | Lateness grows with load. Duplicate wakers show up as extra polls |

### Tokio

| # | Experiment | Setup | Measure | Expect |
| --- | --- | --- | --- | --- |
| T1 | Runtime flavor | G3 and G4 on `current_thread`, then `multi_thread` with 1, 2, 4, 8 workers | Throughput and latency per flavor | `current_thread` is the closest to Gracio. Extra workers cost more than they give on tiny tasks |
| T2 | Blocking in async | One task spins 5 ms among 10k timers, on each flavor, then moved to `spawn_blocking` | p50 and p99 wake lateness of the other tasks | `current_thread` freezes everyone. `multi_thread` hurts only when every worker is blocked |
| T3 | Time between awaits | Loop with 1 µs, 10 µs, 100 µs, 1 ms of work between `.await`s | Tail latency of a parallel ping task | Tail latency grows sharply past roughly 100 µs, matching the usual rule of thumb |
| T4 | `Send` cost | Same workload with `spawn` and with `LocalSet` and `spawn_local` | Tasks per second | `spawn_local` is faster. Compare the gap with Gracio's `Rc` design |
| T5 | Judge concurrency | Judge-style run of child processes with a `Semaphore` of 1 up to 2x cores | Wall time versus CPU time per test, and false-TLE rate | Wall time inflates once concurrency passes physical cores. CPU time stays stable |

### Rayon

| # | Experiment | Setup | Measure | Expect |
| --- | --- | --- | --- | --- |
| R1 | Parallel crossover | `sum` of `map` over items costing 10 ns up to 100 µs, with growing N | Smallest N where `par_iter` beats `iter` | Cheap items need a large N. `with_min_len` moves the crossover down |
| R2 | Thread scaling | Same CPU-bound job with `num_threads` 1 to 2x logical cores | Speedup curve against thread count | Near linear to physical cores, then flat or worse on hyperthread siblings |
| R3 | Lock versus reduce | Sum with a shared `Mutex`, an atomic, and `sum()` | Throughput at each thread count | The lock gets slower as threads are added. `sum()` scales |
| R4 | Recursion cutoff | Parallel quicksort or Fibonacci with `join` and a sequential cutoff sweep | Time against cutoff size | A U-shaped curve. Too small a cutoff drowns in spawn overhead |
| R5 | Pool isolation | Global pool versus a dedicated `ThreadPool` plus `install`, under a noisy neighbour | Latency of the isolated job | The dedicated pool keeps latency stable. `install` adds a small fixed cost |
| R6 | Iterator source | `par_iter` on a `Vec`, `par_bridge` on a plain iterator, `par_chunks` | Throughput and output order | Indexed sources split evenly. `par_bridge` is slower and unordered |

## Experiments with all three

These run one workload on every runtime, so the differences between concurrency and parallelism show up in the numbers.

| # | Experiment | Setup | Measure | Expect |
| --- | --- | --- | --- | --- |
| X1 | Waiting workload | 10k tasks that each wait 10 ms, then compute 1 µs. Gracio, Tokio `current_thread`, Tokio `multi_thread`, and Rayon with each job blocking on `thread::sleep` | Total wall time and peak memory | Gracio and both Tokio flavors finish near 10 ms. Rayon needs about 10k / threads x 10 ms |
| X2 | CPU-bound workload | 10k tasks that each spin 1 ms. Same four setups | Total wall time and CPU utilisation | Gracio needs about 10 s on one core. Tokio `multi_thread` and Rayon approach 10 s / cores, with Rayon lower overhead |
| X3 | One blocking call | 1,000 sleeping tasks plus one task that blocks 50 ms. Gracio, Tokio `current_thread`, Tokio `multi_thread` | Lateness of the other 1,000 wakeups | Gracio and `current_thread` slip by about 50 ms. `multi_thread` slips only when all workers block |
| X4 | Gracio with Rayon | Offload a 5 ms computation from a Gracio task to a Rayon pool. Return the result over a `std` channel that Gracio polls with `try_recv` and `sleep` | Added latency, CPU burn while waiting, throughput | Latency is capped by the polling interval. Compare against the Tokio plus Rayon `oneshot` pattern |
| X5 | Tokio and Rayon sizing | Server-style load: N connections, each request does a short await and then a heavy `par_iter`. Grid of Tokio workers x Rayon threads | Throughput and p50, p99 latency | Best results with the two pools sized to share the cores, not each set to the core count |

### Suggested order

1. X1 and X2. They are cheap and make the three models concrete.
2. G1 and G2. They test likely bugs, and each has a small fix to re-measure.
3. R1 and R2. They show where Rayon starts to pay off on your machine.
4. T2 and T3, then X3. They quantify what blocking costs in each runtime.
5. X4 and X5 last. They need new code, and X4 shows whether Gracio needs a cross-thread waker.

For method: use Divan or Criterion for micro-benchmarks and `hyperfine` for whole programs, profile with `samply`, build with the existing release profile (`lto` and `codegen-units = 1`), and pin to physical cores. Record CPU time as well as wall time so noise does not hide real differences.

## What Gracio still lacks to be a small Tokio

Gracio has the core loop (executor, wakers, timers, a bounded channel, `JoinHandle`) but cannot yet run a real program: it has no I/O, no cancellation, no way to wake it from another thread, and a few correctness gaps. The list below is ordered by priority. "Must" items are needed before the name fits, "Should" items make it comfortable to use, and "Later" items are parity work.

| Priority | Gap | What is missing today | Why it matters |
| --- | --- | --- | --- |
| Must | Known behaviour fixes | LIFO waker pop, timers drained only on an empty queue, duplicate timer wakers, slab that never shrinks (experiments G1, G2, G3, G5) | Correctness comes before new features |
| Must | Channel close | No `Sender::close` or drop-to-close, and `recv` cannot return `Option` | Workers can only exit by knowing their job count, as in `worker_pool` |
| Must | Cancellation and panics | No `abort`, and dropping a `JoinHandle` leaves the task running. Tasks are polled without `catch_unwind`, so one panic unwinds out of `run_blocking` | Real programs need to stop tasks and survive a failing one (a `JoinError`) |
| Must | `yield_now` and a poll budget | A task that never awaits cannot be interrupted, and there is no way to yield on purpose | CPU loops need a polite yield, and the README already lists a budget as a TODO |
| Must | `select!`, `join!`, `timeout` | The `timeout` example hand-rolls the race | These are the main way to combine futures |
| Must | I/O reactor | No sockets, no epoll or kqueue. The idle loop is `thread::sleep` | Without it Gracio cannot serve a connection. Using `libc` or raw FFI breaks the zero-dependency promise, so decide that first |
| Must | Cross-thread wake and `spawn_blocking` | Wakers are `Rc` based and state is thread-local, so no other thread can wake the executor | Needed for blocking work, for Rayon offload (experiment X4), and for timers plus I/O in one park |
| Should | More channels | No `oneshot`, unbounded `mpsc`, `watch` or `broadcast` | Most request and response code uses `oneshot` |
| Should | Async sync primitives | No `Mutex`, `Semaphore`, `Notify`, `Barrier` | A `Semaphore` is what bounds concurrency in a judge |
| Should | Timer API | No `interval`, `sleep_until`, reset, and dropping a `Sleep` does not remove its timer entry | Dropped timers leak entries and wake dead tasks |
| Should | I/O traits and streams | No `AsyncRead`, `AsyncWrite`, `Stream`, or helpers like `read_exact` and `copy` | Needed to write protocol code over the reactor |
| Should | Runtime handle and shutdown | No builder, no handle to spawn from outside, and remaining tasks are dropped silently when main finishes | Graceful shutdown and embedding in other programs |
| Should | Observability | No task ids or names, no counts, no poll-time metrics | A runtime you cannot inspect is hard to trust (compare `tokio-console`) |
| Later | `JoinSet` | Listed in the README as a TODO | Convenient structured fan-out and collection |
| Later | Multi-thread scheduler | Single-threaded only, tasks are not `Send` | Work stealing across cores. The deques from the Rayon plan below could be reused |
| Later | Process, fs, signals | None | Mostly built on the blocking pool once it exists |
| Later | Release hygiene | README clone URL still reads `user/gracio`, the license link points to `LICENSE-MIT` but the file is `LICENSE`, no CI, `miri` not run on the unsafe waker, no crates.io release | Trust and reproducibility |

A concrete bar for "small Tokio": a TCP echo server and the judge example both run on Gracio with timeouts, cancellation and graceful shutdown, and the test suite passes under `miri`.

## Plan for a small Rayon

Build it as a sibling crate to Gracio (working name `gracio-par`, rename freely) in the same style: zero dependencies, `std` threads only, and `unsafe` confined to one small module. Scope is fork-join plus data-parallel iterators over slices, ranges and `Vec`. Each phase ends with a measurable check, and the real Rayon is the baseline in every benchmark.

| Phase | Deliverable | Done when |
| --- | --- | --- |
| P0 Pool core | `N` worker threads, a global injector queue (`Mutex<VecDeque>` plus `Condvar`), `ThreadPoolBuilder::num_threads`, `spawn` of `FnOnce + Send + 'static`, clean shutdown | A million spawned jobs all run, and idle workers use no CPU |
| P1 `join` | Stack-allocated job with a latch, a deque per worker (owner pops newest, thieves take oldest), the waiting caller runs other jobs, panics re-raised after both halves finish | Parallel Fibonacci and quicksort are correct including panic cases, and the cutoff curve from R4 is reproduced |
| P2 `scope` | `scope(\|s\| s.spawn(...))` that borrows local data, with a counting latch | Borrow rules are enforced by `compile_fail` doc tests and the suite is clean under `miri` |
| P3 Parallel iterators | `par_iter` and `par_iter_mut` on slices, `Vec` and `Range<usize>` with `map`, `filter`, `for_each`, `fold`, `reduce`, `sum`, ordered `collect`, `with_min_len`. Splitting is adaptive and built on `join` | Results match the sequential versions on randomized tests, and R1, R3 and R6 run unchanged |
| P4 Scheduler quality | Random victim choice, parking with a sleepy-worker counter, then a lock-free Chase-Lev deque swapped in behind the same interface | R1, R2 and R4 land within an agreed margin of real Rayon (suggested start: 1.3x) |
| P5 Pool control | Lazily created global pool, `install`, `current_num_threads`, an environment variable for thread count, a panic handler | R5 shows a dedicated pool keeps its latency under a noisy neighbour |
| P6 Gracio bridge | A call that runs a closure on the pool and returns a future Gracio can await | X4 shows wake-up without a polling loop. This needs the "cross-thread wake" item from the gap list above |

### Risks

- **Lifetime erasure in `join` and `scope`.** This is the part that makes borrowing safe, and the likeliest source of unsoundness. Keep it in one module, test it under `miri`, and add randomized stress tests.
- **Deadlock when workers block.** A job that waits on a lock or channel held by another job can stall the pool, as in real Rayon. Document it and add a test that shows it.
- **Unfair benchmarks.** Compare against real Rayon on the same pinned cores, with the same build profile, and report CPU time as well as wall time.

### How it fits with Gracio

Gracio stays the place for waiting and `gracio-par` the place for computing, mirroring Tokio plus Rayon. Once P6 and the cross-thread wake exist, experiment X5 can be repeated with the small pair against the real pair.
