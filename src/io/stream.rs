use core::future::Future;
use core::pin::Pin;
use core::task::{Context, Poll};

pub trait Stream {
    type Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>>;

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

pub trait StreamExt: Stream {
    fn next(&mut self) -> Next<'_, Self>
    where
        Self: Unpin,
    {
        Next { stream: self }
    }

    fn map<F, B>(self, f: F) -> Map<Self, F>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> B,
    {
        Map { stream: self, f }
    }

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

    fn take(self, n: usize) -> Take<Self>
    where
        Self: Sized,
    {
        Take {
            stream: self,
            remaining: n,
        }
    }

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

pub struct Next<'a, S: ?Sized> {
    stream: &'a mut S,
}

impl<S: Stream + Unpin + ?Sized> Future for Next<'_, S> {
    type Output = Option<S::Item>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut *self.stream).poll_next(cx)
    }
}

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
