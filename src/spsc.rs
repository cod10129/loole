//! A specialized SPSC-only channel for optimization.

// A lot of this code is pulled from the main channel.

// FIXME(cod10129):
// Do we need the IDs in the Queue at all?

#![allow(missing_docs)]
#![allow(clippy::len_without_is_empty)]

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use crate::{Mutex, MutexGuard, Signal, SyncSignal, Queue};

#[doc(no_inline)]
pub use crate::{TrySendError, TryRecvError, RecvError, SendError};

fn channel<T>(cap: Option<usize>) -> (Sender<T>, Receiver<T>) {
    let shared_state = Arc::new(Mutex::new(SharedState::new(cap)));
    let sender = Sender::new(Arc::clone(&shared_state));
    let receiver = Receiver::new(shared_state);
    (sender, receiver)
}

pub fn bounded<T>(cap: usize) -> (Sender<T>, Receiver<T>) {
    channel(Some(cap))
}

pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    channel(None)
}

/// A version of `crate::SharedState` for an SPSC channel.
struct SharedState<T> {
    // recv singular because there can only be one receiver
    pending_recv: Option<Signal>,
    // only one Sender so we don't need a queue
    pending_send: Option<(usize, T, Option<Signal>)>,
    queue: Queue<T>,
    closed: bool,
    cap: Option<usize>,
    next_id: usize,
}

impl<T> SharedState<T> {
    fn new(cap: Option<usize>) -> Self {
        Self {
            pending_recv: None,
            pending_send: None,
            queue: Queue::new(),
            closed: false,
            cap,
            next_id: 1,
        }
    }

    fn len(&self) -> usize {
        self.queue.len()
    }

    fn is_full(&self) -> bool {
        Some(self.len()) == self.cap
    }

    fn close(&mut self) -> bool {
        let was_closed = self.closed;
        self.closed = true;
        if let Some(signal) = &self.pending_recv {
            signal.wake_by_ref();
        }
        if let Some((_, _, Some(signal))) = &self.pending_send {
            signal.wake_by_ref();
        }
        !was_closed
    }
}

enum TrySendResult<'a, T> {
    Ok,
    Disconnected(T),
    Full(T, MutexGuard<'a, SharedState<T>>),
}

fn try_send<T>(m: T, id: usize, mut guard: MutexGuard<'_, SharedState<T>>) -> TrySendResult<'_, T> {
    if guard.closed {
        return TrySendResult::Disconnected(m);
    }
    if !guard.is_full() {
        guard.queue.enqueue(id, m);
        let pending_recv = guard.pending_recv.take();
        drop(guard);
        if let Some(signal) = pending_recv {
            signal.wake();
        }
        return TrySendResult::Ok;
    } else if guard.cap == Some(0) {
        if let Some(signal) = guard.pending_recv.take() {
            // FIXME(cod10129) This should be unreachable
            debug_assert!(guard.pending_send.is_none());
            guard.pending_send = Some((id, m, None));
            drop(guard);
            signal.wake();
            return TrySendResult::Ok;
        }
    }
    TrySendResult::Full(m, guard)
}

enum TryRecvResult<'a, T> {
    Ok(T),
    Disconnected,
    Empty(MutexGuard<'a, SharedState<T>>),
}

fn try_recv<T>(mut guard: MutexGuard<'_, SharedState<T>>) -> TryRecvResult<'_, T> {
    if let Some((_, value)) = guard.queue.dequeue() {
        if let Some((id, value, signal)) = guard.pending_send.take() {
            guard.queue.enqueue(id, value);
            if let Some(signal) = signal {
                drop(guard);
                signal.wake();
            }
        }
        return TryRecvResult::Ok(value);
    } else if guard.cap == Some(0) {
        if let Some((_, value, signal)) = guard.pending_send.take() {
            if let Some(signal) = signal {
                drop(guard);
                signal.wake();
            }
            return TryRecvResult::Ok(value);
        }
    }
    if guard.closed {
        return TryRecvResult::Disconnected;
    }
    TryRecvResult::Empty(guard)
}

/// An SPSC receiver.
///
/// This type does not implement `Clone`.
pub struct Receiver<T> {
    shared_state: Arc<Mutex<SharedState<T>>>,
    next_id: AtomicUsize,
}

impl<T> std::fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Receiver").finish()
    }
}

impl<T> Receiver<T> {
    fn new(shared_state: Arc<Mutex<SharedState<T>>>) -> Self {
        Self {
            shared_state,
            next_id: AtomicUsize::new(1),
        }
    }

    fn get_next_id(&self) -> usize {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn close(&self) -> bool {
        self.shared_state.lock().close()
    }

    pub fn is_closed(&self) -> bool {
        self.shared_state.lock().closed
    }

    pub fn len(&self) -> usize {
        self.shared_state.lock().len()
    }

    pub fn capacity(&self) -> Option<usize> {
        self.shared_state.lock().cap
    }

    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        match try_recv(self.shared_state.lock()) {
            TryRecvResult::Ok(value) => Ok(value),
            TryRecvResult::Disconnected => Err(TryRecvError::Disconnected),
            TryRecvResult::Empty(_) => Err(TryRecvError::Empty),
        }
    }

    pub fn recv(&self) -> Result<T, RecvError> {
        loop {
            let mut guard = match try_recv(self.shared_state.lock()) {
                TryRecvResult::Ok(value) => return Ok(value),
                TryRecvResult::Disconnected => return Err(RecvError::Disconnected),
                TryRecvResult::Empty(guard) => guard,
            };
            // FIXME(cod10129) remove this if unneeded
            let id = self.get_next_id();
            let sync_signal = SyncSignal::new();
            // FIXME(cod10129) this should be unreachable
            debug_assert!(guard.pending_recv.is_none());
            guard.pending_recv = Some(Signal::Sync(sync_signal.clone()));
            drop(guard);
            sync_signal.wait();
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.shared_state.lock().close();
    }
}

/// An SPSC sender.
///
/// This type does not implement `Clone`.
pub struct Sender<T> {
    shared_state: Arc<Mutex<SharedState<T>>>,
    next_id: AtomicUsize,
}

impl<T> std::fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sender").finish()
    }
}

impl<T> Sender<T> {
    fn new(shared_state: Arc<Mutex<SharedState<T>>>) -> Self {
        Self {
            shared_state,
            next_id: AtomicUsize::new(1),
        }
    }

    fn get_next_id(&self) -> usize {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn close(&self) -> bool {
        self.shared_state.lock().close()
    }

    pub fn is_closed(&self) -> bool {
        self.shared_state.lock().closed
    }

    pub fn len(&self) -> usize {
        self.shared_state.lock().len()
    }

    pub fn capacity(&self) -> Option<usize> {
        self.shared_state.lock().cap
    }

    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        match try_send(value, self.get_next_id(), self.shared_state.lock()) {
            TrySendResult::Ok => Ok(()),
            TrySendResult::Disconnected(value) => Err(TrySendError::Disconnected(value)),
            TrySendResult::Full(value, _) => Err(TrySendError::Full(value)),
        }
    }

    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        let id = self.get_next_id();
        let (value, mut guard) = match try_send(value, id, self.shared_state.lock()) {
            TrySendResult::Ok => return Ok(()),
            TrySendResult::Disconnected(value) => return Err(SendError(value)),
            TrySendResult::Full(value, guard) => (value, guard),
        };
        let sync_signal = SyncSignal::new();
        // FIXME(cod10129) This should be unreachable
        debug_assert!(guard.pending_send.is_none());
        guard.pending_send = Some((id, value, Some(Signal::Sync(sync_signal.clone()))));
        drop(guard);
        sync_signal.wait();
        let mut guard = self.shared_state.lock();
        if guard.closed {
            if let Some((_, value, Some(_))) = guard.pending_send.take() {
                return Err(SendError(value));
            }
        }
        Ok(())
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.shared_state.lock().close();
    }
}
