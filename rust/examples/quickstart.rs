//! The Rust quick start from README.md. Run: cargo run --example quickstart

use lombokasync::{block_on, mpsc, spawn, timeout};
use std::time::Duration;

fn main() {
    let total = block_on(async {
        let (tx, mut rx) = mpsc::bounded::<u32>(8).unwrap();
        for i in 0..3 {
            let tx = tx.clone();
            spawn(async move { tx.send(i).await.unwrap() });
        }
        drop(tx);
        let mut sum = 0;
        while let Some(v) = rx.recv().await {
            sum += v;
        }
        sum
    });
    assert_eq!(total, 3);
    assert_eq!(
        block_on(timeout(Duration::from_millis(50), async { 1 })),
        Ok(1)
    );
    println!("sum = {total}");
}
