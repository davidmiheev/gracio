//! Recursive spawn: each task spawns more tasks, building a tree.
//!
//! Demonstrates that the executor can handle deeply nested futures and
//! that `JoinHandle::await` correctly chains through many levels of
//! spawned work.  We build a binary tree of depth `D` where each leaf
//! returns 1 and every internal node returns the sum of its children + 1.
//!
//! Run with:
//! ```bash
//! cargo run --example recursive_spawn
//! ```

use gracio::{run_blocking, sleep, spawn};
use std::time::Duration;

fn main() {
    run_blocking(async {
        const DEPTH: u32 = 5;

        let start = std::time::Instant::now();
        let total = build_tree(DEPTH).await;
        let elapsed = start.elapsed();

        println!(
            "\ndepth-{DEPTH} binary tree summed in {elapsed:?}, total = {total}"
        );

        // Tree has 2^(D+1) - 1 nodes total.
        let expected = 2u64.pow(DEPTH + 1) - 1;
        assert_eq!(total, expected);
        println!("✓ tree sum matches expected 2^(D+1) - 1 = {expected}");
    });
}

/// Recursively spawn a binary subtree and return its sum.
///
/// `depth = 0` is a leaf that returns 1.
/// `depth > 0` spawns two children concurrently and returns their sum + 1.
async fn build_tree(depth: u32) -> u64 {
    if depth == 0 {
        // Simulate work proportional to depth so the log shows the
        // recursive scheduling interleaving.
        sleep(Duration::from_millis(2)).await;
        return 1;
    }

    // Spawn children concurrently — both branches run in parallel.
    let left = spawn(build_tree(depth - 1));
    let right = spawn(build_tree(depth - 1));

    let left_sum = left.await;
    let right_sum = right.await;
    let sum = left_sum + right_sum + 1;
    println!(
        "  [depth {depth}] node = {sum}, left = {left_sum}, right = {right_sum}"
    );
    sum
}
