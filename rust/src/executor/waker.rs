//! Custom Waker implementation for the single-threaded executor.

use std::task::{RawWaker, RawWakerVTable, Waker};
use std::sync::Arc;

/// Data held by the waker — the task id and a flag to mark it ready.
pub(crate) struct WakerData {
    pub task_id: usize,
    pub wake_flag: std::sync::atomic::AtomicBool,
}

impl WakerData {
    pub fn new(task_id: usize) -> Arc<Self> {
        Arc::new(WakerData {
            task_id,
            wake_flag: std::sync::atomic::AtomicBool::new(true), // start ready
        })
    }
}

const VTABLE: RawWakerVTable = RawWakerVTable::new(
    waker_clone,
    waker_wake,
    waker_wake_by_ref,
    waker_drop,
);

unsafe fn waker_clone(data: *const ()) -> RawWaker {
    let arc = unsafe { Arc::from_raw(data as *const WakerData) };
    let cloned = arc.clone();
    std::mem::forget(arc); // don't decrement original
    RawWaker::new(Arc::into_raw(cloned) as *const (), &VTABLE)
}

unsafe fn waker_wake(data: *const ()) {
    let arc = unsafe { Arc::from_raw(data as *const WakerData) };
    arc.wake_flag.store(true, std::sync::atomic::Ordering::Release);
    // arc drops here, decrementing refcount
}

unsafe fn waker_wake_by_ref(data: *const ()) {
    let arc = unsafe { Arc::from_raw(data as *const WakerData) };
    arc.wake_flag.store(true, std::sync::atomic::Ordering::Release);
    std::mem::forget(arc); // don't decrement
}

unsafe fn waker_drop(data: *const ()) {
    let _arc = unsafe { Arc::from_raw(data as *const WakerData) };
    // arc drops here
}

pub(crate) fn create_waker(data: Arc<WakerData>) -> Waker {
    let raw = RawWaker::new(Arc::into_raw(data) as *const (), &VTABLE);
    unsafe { Waker::from_raw(raw) }
}
