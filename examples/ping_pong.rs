//! Ping-pong: two tasks trading a single value back and forth over a
//! pair of channels.
//!
//! This is the canonical "two-tasking" pattern: each task waits on its
//! own inbound channel and sends on its own outbound channel.  No
//! mutex, no shared state, no atomic — just two channels wired
//! together, one in each direction.
//!
//! Run with:
//! ```bash
//! cargo run --example ping_pong
//! ```

use gracio::{bounded_channel, run_blocking, sleep, spawn};
use std::time::Duration;

const ROUNDS: usize = 5;

fn main() {
    run_blocking(async {
        // Two channels, one in each direction.
        //   alice_to_bob: Alice sends, Bob receives.
        //   bob_to_alice: Bob sends, Alice receives.
        let (alice_to_bob_tx, alice_to_bob_rx) = bounded_channel::<&'static str>(1);
        let (bob_to_alice_tx, bob_to_alice_rx) = bounded_channel::<&'static str>(1);

        // Alice starts the conversation by sending "ping".
        alice_to_bob_tx.send("ping").await;

        // Spawn Bob: receives from alice_to_bob_rx, replies on bob_to_alice_tx.
        let bob = spawn(async move {
            for round in 1..=ROUNDS {
                let msg = alice_to_bob_rx.recv().await;
                println!("[Bob]   round {round}: received «{msg}»");
                sleep(Duration::from_millis(15)).await;
                let reply = match msg {
                    "ping" => "pong",
                    "pong" => "ping",
                    _ => "?",
                };
                bob_to_alice_tx.send(reply).await;
                println!("[Bob]   round {round}: sent «{reply}»");
            }
        });

        // Alice (the main task): receives from bob_to_alice_rx, replies on
        // alice_to_bob_tx.  She runs ROUNDS rounds so she sends the
        // final "pong" back and the conversation can wind down cleanly.
        for round in 1..=ROUNDS {
            let msg = bob_to_alice_rx.recv().await;
            println!("[Alice] round {round}: received «{msg}»");
            sleep(Duration::from_millis(15)).await;
            let reply = match msg {
                "ping" => "pong",
                "pong" => "ping",
                _ => "?",
            };
            alice_to_bob_tx.send(reply).await;
            println!("[Alice] round {round}: sent «{reply}»");
        }

        bob.await;
        println!("\n✓ {ROUNDS} ping-pong rounds completed");
    });
}
