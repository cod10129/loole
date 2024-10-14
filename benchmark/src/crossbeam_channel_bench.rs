use crossbeam_channel::{bounded, unbounded, Receiver, RecvError, SendError, Sender};
use std::{future::Future, thread};

use crate::bench_utils::{calculate_benchmark_result, BenchError, BenchResult, JoinHandle};

pub fn crate_name() -> &'static str {
    "crossbeam-channel"
}

pub fn send<T>(tx: &Sender<T>, msg: T) -> Result<(), SendError<T>> {
    tx.send(msg)
}

pub fn recv<T>(rx: &Receiver<T>) -> Result<T, RecvError> {
    rx.recv()
}

async fn bench_helper<T, S, R>(
    senders_no: usize,
    receivers_no: usize,
    cap: Option<usize>,
    msg_no: usize,
    create_sender: S,
    create_receiver: R,
) -> Result<BenchResult, BenchError>
where
    T: From<usize> + Send + 'static,
    S: Fn(Sender<T>, usize) -> JoinHandle<usize>,
    R: Fn(Receiver<T>) -> JoinHandle<usize>,
{
    let (tx, rx) = cap.map_or_else(unbounded::<T>, bounded);
    let senders = create_senders(tx, senders_no, msg_no, create_sender);
    let receivers = create_receivers(rx, receivers_no, create_receiver);
    Ok(calculate_benchmark_result(senders, receivers).await)
}

fn create_senders<T, S>(
    tx: Sender<T>,
    senders_no: usize,
    msg_no: usize,
    create_sender: S,
) -> Vec<JoinHandle<usize>>
where
    S: Fn(Sender<T>, usize) -> JoinHandle<usize>,
{
    let mut senders = Vec::with_capacity(senders_no);
    for i in 0..senders_no {
        let n = msg_no / senders_no + if i < msg_no % senders_no { 1 } else { 0 };
        senders.push(create_sender(tx.clone(), n));
    }
    senders
}

fn create_receivers<T, R>(
    rx: Receiver<T>,
    receivers_no: usize,
    create_receiver: R,
) -> Vec<JoinHandle<usize>>
where
    R: Fn(Receiver<T>) -> JoinHandle<usize>,
{
    let mut receivers = Vec::with_capacity(receivers_no);
    for _ in 0..receivers_no {
        receivers.push(create_receiver(rx.clone()));
    }
    receivers
}

fn create_sync_sender<T>(tx: Sender<T>, n: usize) -> JoinHandle<usize>
where
    T: From<usize> + Send + 'static,
{
    JoinHandle::Sync(thread::spawn(move || {
        let mut error_count = 0;
        let mut sent_count = 0;
        for k in 0..n {
            match send(&tx, k.into()) {
                Ok(_) => sent_count += 1,
                Err(_) => error_count += 1,
            }
        }
        if error_count > 0 {
            println!("Sync sender encountered {} errors", error_count);
        }
        sent_count
    }))
}

fn create_sync_receiver<T>(rx: Receiver<T>) -> JoinHandle<usize>
where
    T: From<usize> + Send + 'static,
{
    JoinHandle::Sync(thread::spawn(move || {
        let mut received_count = 0;
        loop {
            match recv(&rx) {
                Ok(_) => received_count += 1,
                Err(_) => break,
            }
        }
        received_count
    }))
}

pub async fn bench_sync_sync<T>(
    senders_no: usize,
    receivers_no: usize,
    buffer_size: Option<usize>,
    msg_no: usize,
) -> Result<BenchResult, BenchError>
where
    T: From<usize> + Send + 'static,
{
    bench_helper(
        senders_no,
        receivers_no,
        buffer_size,
        msg_no,
        create_sync_sender::<T>,
        create_sync_receiver::<T>,
    )
    .await
}
