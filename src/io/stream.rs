//! Stream trait and combinators for asynchronous sequences.
//!
//! This module defines runite's lightweight [`Stream`] abstraction and
//! [`StreamExt`] combinators. Streams yield values over time on the current
//! thread and are used by APIs such as channel receivers and line-oriented I/O.
//!
//! # Examples
//!
//! ```
//! use core::pin::Pin;
//! use core::task::{Context, Poll};
//!
//! use runite::io::{Stream, StreamExt};
//!
//! struct Counter {
//!     next: u8,
//!     end: u8,
//! }
//!
//! impl Stream for Counter {
//!     type Item = u8;
//!
//!     fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//!         if self.next == self.end {
//!             Poll::Ready(None)
//!         } else {
//!             let item = self.next;
//!             self.next += 1;
//!             Poll::Ready(Some(item))
//!         }
//!     }
//! }
//!
//! runite::queue_future(async {
//!     let values = Counter { next: 0, end: 6 }
//!         .filter(|item| item % 2 == 0)
//!         .map(|item| item * 10)
//!         .collect::<Vec<_>>()
//!         .await;
//!     assert_eq!(values, [0, 20, 40]);
//! });
//! runite::run();
//! ```

use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

/// Asynchronous sequence of values.
///
/// A stream is the asynchronous counterpart to [`Iterator`]. Each call to
/// [`poll_next`](Self::poll_next) attempts to produce the next item without
/// blocking. Returning [`Poll::Pending`] means the stream stored the current
/// waker and will wake it when another item, or the end of the stream, may be
/// available.
///
/// # Examples
///
/// ```
/// use core::pin::Pin;
/// use core::task::{Context, Poll};
///
/// use runite::io::{Stream, StreamExt};
///
/// struct Counter { next: u8, end: u8 }
///
/// impl Stream for Counter {
///     type Item = u8;
///
///     fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
///         if self.next == self.end {
///             Poll::Ready(None)
///         } else {
///             let item = self.next;
///             self.next += 1;
///             Poll::Ready(Some(item))
///         }
///     }
///
///     fn size_hint(&self) -> (usize, Option<usize>) {
///         let remaining = (self.end - self.next) as usize;
///         (remaining, Some(remaining))
///     }
/// }
///
/// runite::queue_future(async {
///     let values = Counter { next: 0, end: 3 }.collect::<Vec<_>>().await;
///     assert_eq!(values, [0, 1, 2]);
/// });
/// runite::run();
/// ```
pub trait Stream {
    /// The type of item yielded by this stream.
    type Item;

    /// Attempts to resolve the next item in the stream.
    ///
    /// Return `Poll::Ready(Some(item))` when an item is available,
    /// `Poll::Ready(None)` after the stream has ended, or [`Poll::Pending`]
    /// when the stream cannot currently make progress.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>>;

    /// Returns bounds on the remaining length of the stream.
    ///
    /// The first element is a lower bound and the second is an optional upper
    /// bound, following [`Iterator::size_hint`].
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

/// Extension methods for [`Stream`].
///
/// This trait is implemented for all streams and provides futures and stream
/// combinators similar to those in the broader Rust async ecosystem.
///
/// # Examples
///
/// ```
/// # use core::pin::Pin;
/// # use core::task::{Context, Poll};
/// # use runite::io::{Stream, StreamExt};
/// # struct Counter { next: u8, end: u8 }
/// # impl Stream for Counter {
/// #     type Item = u8;
/// #     fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
/// #         if self.next == self.end { Poll::Ready(None) } else { let item = self.next; self.next += 1; Poll::Ready(Some(item)) }
/// #     }
/// # }
/// runite::queue_future(async {
///     let values = Counter { next: 0, end: 6 }
///         .skip(1)
///         .take(3)
///         .map(|item| item * 2)
///         .collect::<Vec<_>>()
///         .await;
///     assert_eq!(values, [2, 4, 6]);
/// });
/// runite::run();
/// ```
pub trait StreamExt: Stream {
    /// Returns a future that resolves to the next item from this stream.
    ///
    /// # Examples
    ///
    /// ```
    /// # use core::pin::Pin;
    /// # use core::task::{Context, Poll};
    /// # use runite::io::{Stream, StreamExt};
    /// # struct Counter { next: u8, end: u8 }
    /// # impl Stream for Counter { type Item = u8; fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<u8>> { if self.next == self.end { Poll::Ready(None) } else { let item = self.next; self.next += 1; Poll::Ready(Some(item)) } } }
    /// runite::queue_future(async {
    ///     let mut stream = Counter { next: 4, end: 6 };
    ///     assert_eq!(stream.next().await, Some(4));
    ///     assert_eq!(stream.next().await, Some(5));
    ///     assert_eq!(stream.next().await, None);
    /// });
    /// runite::run();
    /// ```
    fn next(&mut self) -> Next<'_, Self>
    where
        Self: Unpin,
    {
        Next { stream: self }
    }

    /// Creates a stream that transforms each item with `f`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use core::pin::Pin;
    /// # use core::task::{Context, Poll};
    /// # use runite::io::{Stream, StreamExt};
    /// # struct Counter { next: u8, end: u8 }
    /// # impl Stream for Counter { type Item = u8; fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<u8>> { if self.next == self.end { Poll::Ready(None) } else { let item = self.next; self.next += 1; Poll::Ready(Some(item)) } } }
    /// runite::queue_future(async {
    ///     let values = Counter { next: 1, end: 4 }.map(|item| item * 10).collect::<Vec<_>>().await;
    ///     assert_eq!(values, [10, 20, 30]);
    /// });
    /// runite::run();
    /// ```
    fn map<F, B>(self, f: F) -> Map<Self, F>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> B,
    {
        Map { stream: self, f }
    }

    /// Creates a stream that yields only items for which `predicate` returns `true`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use core::pin::Pin;
    /// # use core::task::{Context, Poll};
    /// # use runite::io::{Stream, StreamExt};
    /// # struct Counter { next: u8, end: u8 }
    /// # impl Stream for Counter { type Item = u8; fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<u8>> { if self.next == self.end { Poll::Ready(None) } else { let item = self.next; self.next += 1; Poll::Ready(Some(item)) } } }
    /// runite::queue_future(async {
    ///     let values = Counter { next: 0, end: 6 }.filter(|item| item % 2 == 0).collect::<Vec<_>>().await;
    ///     assert_eq!(values, [0, 2, 4]);
    /// });
    /// runite::run();
    /// ```
    fn filter<F>(self, predicate: F) -> Filter<Self, F>
    where
        Self: Sized,
        F: FnMut(&Self::Item) -> bool,
    {
        Filter {
            stream: self,
            predicate,
        }
    }

    /// Collects all remaining stream items into a collection.
    ///
    /// The collection type must implement [`Default`] and [`Extend`].
    ///
    /// # Examples
    ///
    /// ```
    /// # use core::pin::Pin;
    /// # use core::task::{Context, Poll};
    /// # use runite::io::{Stream, StreamExt};
    /// # struct Counter { next: u8, end: u8 }
    /// # impl Stream for Counter { type Item = u8; fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<u8>> { if self.next == self.end { Poll::Ready(None) } else { let item = self.next; self.next += 1; Poll::Ready(Some(item)) } } }
    /// runite::queue_future(async {
    ///     let values = Counter { next: 2, end: 5 }.collect::<Vec<_>>().await;
    ///     assert_eq!(values, [2, 3, 4]);
    /// });
    /// runite::run();
    /// ```
    fn collect<C>(self) -> Collect<Self, C>
    where
        Self: Sized,
        C: Default + Extend<Self::Item>,
    {
        Collect {
            stream: self,
            collection: C::default(),
        }
    }

    /// Runs an async closure for each remaining item.
    ///
    /// Items are processed sequentially: the next stream item is not polled until
    /// the future returned for the previous item has completed.
    ///
    /// # Examples
    ///
    /// ```
    /// # use core::pin::Pin;
    /// # use core::task::{Context, Poll};
    /// # use std::{cell::RefCell, rc::Rc};
    /// # use runite::io::{Stream, StreamExt};
    /// # struct Counter { next: u8, end: u8 }
    /// # impl Stream for Counter { type Item = u8; fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<u8>> { if self.next == self.end { Poll::Ready(None) } else { let item = self.next; self.next += 1; Poll::Ready(Some(item)) } } }
    /// let seen = Rc::new(RefCell::new(Vec::new()));
    /// let observed = Rc::clone(&seen);
    /// runite::queue_future(async move {
    ///     Counter { next: 0, end: 3 }
    ///         .for_each(|item| {
    ///             let seen = Rc::clone(&seen);
    ///             async move { seen.borrow_mut().push(item) }
    ///         })
    ///         .await;
    /// });
    /// runite::run();
    /// assert_eq!(&*observed.borrow(), &[0, 1, 2]);
    /// ```
    fn for_each<F, Fut>(self, f: F) -> ForEach<Self, F, Fut>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> Fut,
        Fut: Future<Output = ()>,
    {
        ForEach {
            stream: self,
            f,
            pending: None,
        }
    }

    /// Creates a stream that yields at most `n` items.
    ///
    /// # Examples
    ///
    /// ```
    /// # use core::pin::Pin;
    /// # use core::task::{Context, Poll};
    /// # use runite::io::{Stream, StreamExt};
    /// # struct Counter { next: u8, end: u8 }
    /// # impl Stream for Counter { type Item = u8; fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<u8>> { if self.next == self.end { Poll::Ready(None) } else { let item = self.next; self.next += 1; Poll::Ready(Some(item)) } } }
    /// runite::queue_future(async {
    ///     let values = Counter { next: 0, end: 10 }.take(2).collect::<Vec<_>>().await;
    ///     assert_eq!(values, [0, 1]);
    /// });
    /// runite::run();
    /// ```
    fn take(self, n: usize) -> Take<Self>
    where
        Self: Sized,
    {
        Take {
            stream: self,
            remaining: n,
        }
    }

    /// Creates a stream that drops the first `n` items, then yields the rest.
    ///
    /// # Examples
    ///
    /// ```
    /// # use core::pin::Pin;
    /// # use core::task::{Context, Poll};
    /// # use runite::io::{Stream, StreamExt};
    /// # struct Counter { next: u8, end: u8 }
    /// # impl Stream for Counter { type Item = u8; fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<u8>> { if self.next == self.end { Poll::Ready(None) } else { let item = self.next; self.next += 1; Poll::Ready(Some(item)) } } }
    /// runite::queue_future(async {
    ///     let values = Counter { next: 0, end: 5 }.skip(3).collect::<Vec<_>>().await;
    ///     assert_eq!(values, [3, 4]);
    /// });
    /// runite::run();
    /// ```
    fn skip(self, n: usize) -> Skip<Self>
    where
        Self: Sized,
    {
        Skip {
            stream: self,
            remaining: n,
        }
    }
}

impl<S: Stream + ?Sized> StreamExt for S {}

impl<S: Stream + Unpin + ?Sized> Stream for &mut S {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut **self).poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (**self).size_hint()
    }
}

/// Future returned by [`StreamExt::next`].
pub struct Next<'a, S: ?Sized> {
    stream: &'a mut S,
}

impl<S: Stream + Unpin + ?Sized> Future for Next<'_, S> {
    type Output = Option<S::Item>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut *self.stream).poll_next(cx)
    }
}

/// Stream returned by [`StreamExt::map`].
pub struct Map<S, F> {
    stream: S,
    f: F,
}

impl<S, F> Unpin for Map<S, F> {}

impl<S, F, B> Stream for Map<S, F>
where
    S: Stream + Unpin,
    F: FnMut(S::Item) -> B,
{
    type Item = B;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        Pin::new(&mut this.stream)
            .poll_next(cx)
            .map(|item| item.map(&mut this.f))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

/// Stream returned by [`StreamExt::filter`].
pub struct Filter<S, F> {
    stream: S,
    predicate: F,
}

impl<S, F> Unpin for Filter<S, F> {}

impl<S, F> Stream for Filter<S, F>
where
    S: Stream + Unpin,
    F: FnMut(&S::Item) -> bool,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match Pin::new(&mut this.stream).poll_next(cx) {
                Poll::Ready(Some(item)) if (this.predicate)(&item) => {
                    return Poll::Ready(Some(item));
                }
                Poll::Ready(Some(_)) => {}
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Future returned by [`StreamExt::collect`].
pub struct Collect<S, C> {
    stream: S,
    collection: C,
}

impl<S, C> Unpin for Collect<S, C> {}

impl<S, C> Future for Collect<S, C>
where
    S: Stream + Unpin,
    C: Default + Extend<S::Item>,
{
    type Output = C;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            match Pin::new(&mut this.stream).poll_next(cx) {
                Poll::Ready(Some(item)) => this.collection.extend(core::iter::once(item)),
                Poll::Ready(None) => return Poll::Ready(core::mem::take(&mut this.collection)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Future returned by [`StreamExt::for_each`].
pub struct ForEach<S, F, Fut> {
    stream: S,
    f: F,
    pending: Option<Pin<Box<Fut>>>,
}

impl<S, F, Fut> Unpin for ForEach<S, F, Fut> {}

impl<S, F, Fut> Future for ForEach<S, F, Fut>
where
    S: Stream + Unpin,
    F: FnMut(S::Item) -> Fut,
    Fut: Future<Output = ()>,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            if let Some(pending) = this.pending.as_mut() {
                match pending.as_mut().poll(cx) {
                    Poll::Ready(()) => {
                        this.pending = None;
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            match Pin::new(&mut this.stream).poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    this.pending = Some(Box::pin((this.f)(item)));
                }
                Poll::Ready(None) => return Poll::Ready(()),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// Stream returned by [`StreamExt::take`].
pub struct Take<S> {
    stream: S,
    remaining: usize,
}

impl<S> Unpin for Take<S> {}

impl<S> Stream for Take<S>
where
    S: Stream + Unpin,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.remaining == 0 {
            return Poll::Ready(None);
        }
        match Pin::new(&mut this.stream).poll_next(cx) {
            Poll::Ready(Some(item)) => {
                this.remaining -= 1;
                Poll::Ready(Some(item))
            }
            other => other,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (lower, upper) = self.stream.size_hint();
        (
            lower.min(self.remaining),
            upper.map_or(Some(self.remaining), |upper| {
                Some(upper.min(self.remaining))
            }),
        )
    }
}

/// Stream returned by [`StreamExt::skip`].
pub struct Skip<S> {
    stream: S,
    remaining: usize,
}

impl<S> Unpin for Skip<S> {}

impl<S> Stream for Skip<S>
where
    S: Stream + Unpin,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        while this.remaining > 0 {
            match Pin::new(&mut this.stream).poll_next(cx) {
                Poll::Ready(Some(_)) => this.remaining -= 1,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
        Pin::new(&mut this.stream).poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (lower, upper) = self.stream.size_hint();
        (
            lower.saturating_sub(self.remaining),
            upper.map(|upper| upper.saturating_sub(self.remaining)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{Stream, StreamExt};
    use core::pin::Pin;
    use core::task::{Context, Poll};
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use crate::{queue_future, queue_task, run};

    struct VecDequeStream<T> {
        items: VecDeque<T>,
    }

    impl<T> Unpin for VecDequeStream<T> {}

    impl<T> Stream for VecDequeStream<T> {
        type Item = T;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Ready(self.get_mut().items.pop_front())
        }
    }

    #[test]
    fn stream_ext_map_and_take_compose() {
        let observed = Arc::new(Mutex::new(None::<Vec<u32>>));
        let observed_for_task = Arc::clone(&observed);

        queue_task(move || {
            queue_future(async move {
                let stream = VecDequeStream {
                    items: VecDeque::from(vec![1, 1, 1, 1, 1]),
                };
                let values = stream.map(|x| x * 2).take(3).collect::<Vec<_>>().await;
                *observed_for_task.lock().unwrap() = Some(values);
            });
        });

        run();
        assert_eq!(*observed.lock().unwrap(), Some(vec![2, 2, 2]));
    }
}
