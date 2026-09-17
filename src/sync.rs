//! Thin shim over `std` (or `loom` with the `loom` cargo feature) so the core queue
//! can be model-checked with loom without touching the production code path.

#[cfg(feature = "loom")]
pub(crate) use self::loom_impl::*;
#[cfg(not(feature = "loom"))]
pub(crate) use self::std_impl::*;

#[cfg(feature = "loom")]
mod loom_impl {
    pub(crate) use loom::cell::UnsafeCell;
    pub(crate) use loom::sync::atomic::{
        AtomicBool, AtomicPtr, AtomicU64, AtomicU8, AtomicUsize, Ordering,
    };
    pub(crate) use loom::sync::Arc;
    pub(crate) use loom::thread::yield_now;

    #[inline(always)]
    pub(crate) fn spin_loop() {
        loom::thread::yield_now();
    }
}

#[cfg(not(feature = "loom"))]
mod std_impl {
    pub(crate) use std::hint::spin_loop;
    pub(crate) use std::sync::atomic::{
        AtomicBool, AtomicPtr, AtomicU64, AtomicU8, AtomicUsize, Ordering,
    };
    pub(crate) use std::sync::Arc;
    pub(crate) use std::thread::yield_now;

    /// `std::cell::UnsafeCell` with loom's closure-based accessor API.
    #[repr(transparent)]
    pub(crate) struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

    impl<T> UnsafeCell<T> {
        #[inline(always)]
        pub(crate) const fn new(value: T) -> Self {
            UnsafeCell(std::cell::UnsafeCell::new(value))
        }

        #[inline(always)]
        pub(crate) fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R {
            f(self.0.get())
        }

        #[inline(always)]
        pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
            f(self.0.get())
        }
    }
}

/// Pads and aligns a value to 128 bytes: the cache-line size on Apple Silicon and
/// the adjacent-line prefetch pair on x86, so neighbouring fields never share a line.
#[repr(align(128))]
pub(crate) struct CachePadded<T>(pub(crate) T);

impl<T> std::ops::Deref for CachePadded<T> {
    type Target = T;
    #[inline(always)]
    fn deref(&self) -> &T {
        &self.0
    }
}
