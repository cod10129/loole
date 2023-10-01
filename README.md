# Loole

A safe async/sync multi-producer multi-consumer channel.

![Github Actions](https://github.com/mahdi-shojaee/loole/actions/workflows/rust.yml/badge.svg?branch=master)
[![Documentation](https://docs.rs/loole/badge.svg)](https://docs.rs/loole)
[![Cargo](https://img.shields.io/crates/v/loole.svg)](https://crates.io/crates/loole)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](https://github.com/mahdi-shojaee/loole)

Producers can send and consumers can receive messages asynchronously or synchronously:

- async send -> async receive
- sync send -> sync receive
- async send -> sync receive
- sync send -> async receive

```rust
fn main() {
    let (tx, rx) = loole::unbounded();

    std::thread::spawn(move || {
        for i in (0..10) {
            tx.send(i).unwrap();
        }
    });

    let mut sum = 0;
    while let Ok(i) = rx.recv() {
        sum += i;
    }

    assert_eq!(sum, (0..10).sum());
}
```

## Usage

To use Loole, place the following line under the `[dependencies]` section in your `Cargo.toml`:

```toml
loole = "0.1.1"
```

## Benchmarks

### MPSC

![MPSC](misc/loole-mpsc.png)

### MPMC

![MPMC](misc/loole-mpmc.png)

### SPSC

![SPSC](misc/loole-spsc.png)

## License

Loole is licensed under either of:

- Apache License 2.0, (http://www.apache.org/licenses/LICENSE-2.0)

- MIT license (http://opensource.org/licenses/MIT)
