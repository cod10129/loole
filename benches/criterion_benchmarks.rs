#![allow(dead_code)]

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use loole::{bounded, unbounded, Receiver, RecvError, SendError, Sender};
use loole::spsc::{Receiver as SpscReceiver, Sender as SpscSender};
use std::{error::Error, future::Future, thread, time::Instant};

use std::fmt::{Debug, Display};

const MESSAGES_NO: usize = 1_000_000;
const MESSAGE_SIZE: usize = 256;
const BUFFER_SIZE: Option<usize> = Some(100);

type MsgType = StackType<MESSAGE_SIZE>;

pub struct Type<T: Length>(usize, T);

impl<T: Default + Length> From<usize> for Type<T> {
    #[inline(always)]
    fn from(value: usize) -> Self {
        Self(value, Default::default())
    }
}

impl<T: Length> Debug for Type<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("index: {}, len: {}", self.0, self.1.len()))
    }
}

impl<T: Length> Display for Type<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("index: {}, len: {}", self.0, self.1.len()))
    }
}

pub struct FilledArray<const SIZE: usize>([u8; SIZE]);

impl<const SIZE: usize> Default for FilledArray<SIZE> {
    #[inline(always)]
    fn default() -> Self {
        Self([0; SIZE])
    }
}

pub struct FilledVec<const SIZE: usize>(Vec<u8>);

impl<const SIZE: usize> Default for FilledVec<SIZE> {
    #[inline(always)]
    fn default() -> Self {
        Self(vec![0; SIZE])
    }
}

#[allow(clippy::len_without_is_empty)]
pub trait Length {
    fn len(&self) -> usize;
}

impl<const SIZE: usize> Length for FilledArray<SIZE> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.0.len()
    }
}

impl<const SIZE: usize> Length for FilledVec<SIZE> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.0.len()
    }
}

pub type StackType<const SIZE: usize> = Type<FilledArray<SIZE>>;
pub type HeapType<const SIZE: usize> = Type<FilledVec<SIZE>>;

pub struct BenchResult {
    pub throughput: usize,
}

#[derive(Debug)]
pub enum BenchError {
    ZeroCapacityNotSupported,
    AsyncNotSupported,
    MpmcNotSupported,
}

impl Display for BenchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BenchError::ZeroCapacityNotSupported => write!(f, "zero capacity not supported"),
            BenchError::AsyncNotSupported => write!(f, "async not supported"),
            BenchError::MpmcNotSupported => write!(f, "mpmc not supported"),
        }
    }
}

impl Error for BenchError {}

pub enum JoinHandle<T> {
    Sync(thread::JoinHandle<T>),
    Async(tokio::task::JoinHandle<T>),
}

impl<T> JoinHandle<T> {
    pub async fn join(self) -> T {
        match self {
            JoinHandle::Sync(h) => h.join().unwrap(),
            JoinHandle::Async(h) => h.await.unwrap(),
        }
    }
}

pub async fn calculate_benchmark_result(
    senders: Vec<JoinHandle<usize>>,
    receivers: Vec<JoinHandle<usize>>,
) -> BenchResult {
    let t = Instant::now();
    let mut sent_no = 0;
    for t in senders {
        sent_no += t.join().await;
    }
    let mut recv_no = 0;
    for t in receivers {
        recv_no += t.join().await;
    }
    let elapsed = t.elapsed();
    assert_eq!(sent_no, recv_no);
    let throughput = (recv_no as f32 / elapsed.as_secs_f32()).round() as usize;
    BenchResult { throughput }
}

pub fn crate_name() -> &'static str {
    "loole"
}

pub fn send<T>(tx: &Sender<T>, msg: T) -> Result<(), SendError<T>> {
    tx.send(msg)
}

pub fn recv<T>(rx: &Receiver<T>) -> Result<T, RecvError> {
    rx.recv()
}

pub fn send_async<T: 'static>(
    tx: &Sender<T>,
    msg: T,
) -> impl Future<Output = Result<(), SendError<T>>> + '_ {
    tx.send_async(msg)
}

pub fn recv_async<T>(rx: &Receiver<T>) -> impl Future<Output = Result<T, RecvError>> + '_ {
    rx.recv_async()
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
        for k in 0..n {
            match send(&tx, k.into()) {
                Ok(_) => (),
                Err(_) => println!("error: channel closed at: {}", k),
            }
        }
        n
    }))
}

fn create_async_sender<T>(tx: Sender<T>, n: usize) -> JoinHandle<usize>
where
    T: From<usize> + Send + 'static,
{
    JoinHandle::Async(tokio::spawn(async move {
        for k in 0..n {
            match send_async(&tx, k.into()).await {
                Ok(_) => (),
                Err(_) => println!("error: channel closed at: {}", k),
            }
        }
        n
    }))
}

fn create_sync_receiver<T>(rx: Receiver<T>) -> JoinHandle<usize>
where
    T: From<usize> + Send + 'static,
{
    JoinHandle::Sync(thread::spawn(move || {
        let mut c = 0;
        loop {
            match recv(&rx) {
                Ok(_) => c += 1,
                Err(_) => break c,
            }
        }
    }))
}

fn create_async_receiver<T>(rx: Receiver<T>) -> JoinHandle<usize>
where
    T: From<usize> + Send + 'static,
{
    JoinHandle::Async(tokio::spawn(async move {
        let mut c = 0;
        loop {
            match recv_async(&rx).await {
                Ok(_) => c += 1,
                Err(_) => break c,
            }
        }
    }))
}

fn create_spsc_sender<T>(tx: SpscSender<T>, n: usize) -> JoinHandle<usize>
where 
    T: From<usize> + Send + 'static
{
    JoinHandle::Sync(thread::spawn(move || {
        for k in 0..n {
            match tx.send(k.into()) {
                Ok(_) => (),
                Err(_) => println!("error: channel closed at: {k}"),
            }
        }
        n
    }))
}

fn create_spsc_receiver<T>(rx: SpscReceiver<T>) -> JoinHandle<usize>
where 
    T: From<usize> + Send + 'static
{
    JoinHandle::Sync(thread::spawn(move || {
        let mut c = 0;
        loop {
            match rx.recv() {
                Ok(_) => c += 1,
                Err(_) => break c,
            }
        }
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

pub async fn bench_async_async<T>(
    senders_no: usize,
    receivers_no: usize,
    buffer_size: Option<usize>,
    msg_no: usize,
) -> Result<BenchResult, BenchError>
where
    T: From<usize> + Send + Sync + 'static,
{
    bench_helper(
        senders_no,
        receivers_no,
        buffer_size,
        msg_no,
        create_async_sender::<T>,
        create_async_receiver::<T>,
    )
    .await
}

pub async fn bench_async_sync<T>(
    senders_no: usize,
    receivers_no: usize,
    buffer_size: Option<usize>,
    msg_no: usize,
) -> Result<BenchResult, BenchError>
where
    T: From<usize> + Send + Sync + 'static,
{
    bench_helper(
        senders_no,
        receivers_no,
        buffer_size,
        msg_no,
        create_async_sender::<T>,
        create_sync_receiver::<T>,
    )
    .await
}

pub async fn bench_sync_async<T>(
    senders_no: usize,
    receivers_no: usize,
    buffer_size: Option<usize>,
    msg_no: usize,
) -> Result<BenchResult, BenchError>
where
    T: From<usize> + Send + Sync + 'static,
{
    bench_helper(
        senders_no,
        receivers_no,
        buffer_size,
        msg_no,
        create_sync_sender::<T>,
        create_async_receiver::<T>,
    )
    .await
}

async fn async_select_recv_buffer_0(msg_no: usize) {
    let count = msg_no;

    let (tx1, rx1) = bounded(0);
    let (tx2, rx2) = bounded(0);

    tokio::spawn(async move {
        for i in (0..count).filter(|n| n % 2 == 0) {
            tx1.send_async(i).await.unwrap();
        }
    });
    tokio::spawn(async move {
        for i in (0..count).filter(|n| n % 2 == 1) {
            tx2.send_async(i).await.unwrap();
        }
    });

    let mut result = Vec::new();
    loop {
        let n = tokio::select! {
            n = rx1.recv_async() => n,
            n = rx2.recv_async() => n,
        };
        if let Ok(n) = n {
            result.push(n);
            if result.len() == count {
                break;
            }
        }
    }
    result.sort();
    let expected = (0..count).collect::<Vec<_>>();
    assert_eq!(result.len(), expected.len());
    assert_eq!(result, expected);
}

fn criterion_benchmarks(c: &mut Criterion) {
    let msg_no = black_box(MESSAGES_NO);
    let buffer_size = BUFFER_SIZE;
    let sample_size = 10;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    {
        let mut select = c.benchmark_group("select");
        select.sample_size(sample_size);
        select.bench_function("async_select_recv_buffer_0", |b| {
            b.iter(|| rt.block_on(async_select_recv_buffer_0(msg_no)))
        });
    }

    {
        let mut mpsc = c.benchmark_group("MPSC");
        mpsc.sample_size(sample_size);
        mpsc.bench_function("5000_sync_1_sync", |b| {
            b.iter(|| {
                rt.block_on(bench_sync_sync::<MsgType>(
                    black_box(5_000),
                    black_box(1),
                    black_box(buffer_size),
                    msg_no,
                ))
            })
        });
        mpsc.bench_function("5000_async_1_async", |b| {
            b.iter(|| {
                rt.block_on(bench_async_async::<MsgType>(
                    black_box(5_000),
                    black_box(1),
                    black_box(buffer_size),
                    msg_no,
                ))
            })
        });
        mpsc.bench_function("5000_async_1_sync", |b| {
            b.iter(|| {
                rt.block_on(bench_async_sync::<MsgType>(
                    black_box(5_000),
                    black_box(1),
                    black_box(buffer_size),
                    msg_no,
                ))
            })
        });
        mpsc.bench_function("5000_sync_1_async", |b| {
            b.iter(|| {
                rt.block_on(bench_sync_async::<MsgType>(
                    black_box(5_000),
                    black_box(1),
                    black_box(buffer_size),
                    msg_no,
                ))
            })
        });
    }

    {
        let mut mpmc = c.benchmark_group("MPMC");
        mpmc.sample_size(sample_size);
        mpmc.bench_function("5000_sync_10_sync", |b| {
            b.iter(|| {
                rt.block_on(bench_sync_sync::<MsgType>(
                    black_box(5_000),
                    black_box(10),
                    black_box(buffer_size),
                    msg_no,
                ))
            })
        });
        mpmc.bench_function("5000_async_10_async", |b| {
            b.iter(|| {
                rt.block_on(bench_async_async::<MsgType>(
                    black_box(5_000),
                    black_box(10),
                    black_box(buffer_size),
                    msg_no,
                ))
            })
        });
        mpmc.bench_function("5000_async_10_sync", |b| {
            b.iter(|| {
                rt.block_on(bench_async_sync::<MsgType>(
                    black_box(5_000),
                    black_box(10),
                    black_box(buffer_size),
                    msg_no,
                ))
            })
        });
        mpmc.bench_function("5000_sync_10_async", |b| {
            b.iter(|| {
                rt.block_on(bench_sync_async::<MsgType>(
                    black_box(5_000),
                    black_box(10),
                    black_box(buffer_size),
                    msg_no,
                ))
            })
        });
    }

    {
        let mut spsc = c.benchmark_group("SPSC");
        spsc.sample_size(sample_size);
        spsc.bench_function("1_sync_1_sync", |b| {
            b.iter(|| {
                rt.block_on(bench_sync_sync::<MsgType>(
                    black_box(1),
                    black_box(1),
                    black_box(buffer_size),
                    msg_no,
                ))
            })
        });
        spsc.bench_function("1_async_1_async", |b| {
            b.iter(|| {
                rt.block_on(bench_async_async::<MsgType>(
                    black_box(1),
                    black_box(1),
                    black_box(buffer_size),
                    msg_no,
                ))
            })
        });
        spsc.bench_function("1_async_1_sync", |b| {
            b.iter(|| {
                rt.block_on(bench_async_sync::<MsgType>(
                    black_box(1),
                    black_box(1),
                    black_box(buffer_size),
                    msg_no,
                ))
            })
        });
        spsc.bench_function("1_sync_1_async", |b| {
            b.iter(|| {
                rt.block_on(bench_sync_async::<MsgType>(
                    black_box(1),
                    black_box(1),
                    black_box(buffer_size),
                    msg_no,
                ))
            })
        });
    }

    // Current perf results are HALF the time of SPSC1
    {
        let mut spsc = c.benchmark_group("SPSC2");
        spsc.sample_size(sample_size);
        spsc.bench_function("1_sync_1_sync", |b| {
            b.iter(|| {
                rt.block_on({
                    // let n = msg_no / 1 + if 0 < msg_no % 1 { 1 } else { 0 };
                    let n = msg_no + 1;
                    let cap = black_box(buffer_size).unwrap();
                    let (tx, rx) = loole::spsc::bounded::<usize>(cap);
                    let sender = create_spsc_sender(tx, n);
                    let receiver = create_spsc_receiver(rx);
                    async move {
                        Ok::<_, BenchError>(
                            calculate_benchmark_result(vec![sender], vec![receiver]).await
                        )
                    }
                })
            })
        });
    }
}

criterion_group!(benches, criterion_benchmarks);
criterion_main!(benches);
