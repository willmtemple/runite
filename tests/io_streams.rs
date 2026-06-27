//! Integration tests for public stream combinators.

mod common;

use common::block_on;
use core::pin::Pin;
use core::task::{Context, Poll};
use runite::io::{Stream, StreamExt};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

struct IterStream<T> {
    items: VecDeque<T>,
    events: Option<Rc<RefCell<Vec<String>>>>,
}

impl<T> IterStream<T> {
    fn new(items: impl IntoIterator<Item = T>) -> Self {
        Self {
            items: items.into_iter().collect(),
            events: None,
        }
    }

    fn with_events(items: impl IntoIterator<Item = T>, events: Rc<RefCell<Vec<String>>>) -> Self {
        Self {
            items: items.into_iter().collect(),
            events: Some(events),
        }
    }
}

impl<T> Stream for IterStream<T>
where
    T: std::fmt::Display,
{
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let item = self.items.pop_front();
        if let Some(events) = &self.events {
            match &item {
                Some(item) => events.borrow_mut().push(format!("poll {item}")),
                None => events.borrow_mut().push("poll eof".to_string()),
            }
        }
        Poll::Ready(item)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.items.len(), Some(self.items.len()))
    }
}

impl<T> Unpin for IterStream<T> {}

#[test]
fn stream_combinators_preserve_order_and_size_hints() {
    block_on(|| async {
        let stream = IterStream::new(0..8).skip(1).take(5);
        assert_eq!(stream.size_hint(), (5, Some(5)));

        let values = stream
            .filter(|item| item % 2 == 1)
            .map(|item| item * 10)
            .collect::<Vec<_>>()
            .await;

        assert_eq!(values, [10, 30, 50]);
    });
}

#[test]
fn next_and_take_stop_polling_after_termination() {
    block_on(|| async {
        let events = Rc::new(RefCell::new(Vec::new()));
        let mut stream = IterStream::with_events(["a", "b", "c"], Rc::clone(&events));

        assert_eq!(stream.next().await, Some("a"));
        assert_eq!(stream.next().await, Some("b"));

        let rest = stream.take(0).collect::<Vec<_>>().await;
        assert!(rest.is_empty());
        assert_eq!(&*events.borrow(), &["poll a", "poll b"]);
    });
}

#[test]
fn for_each_waits_for_item_future_before_polling_next_item() {
    block_on(|| async {
        let events = Rc::new(RefCell::new(Vec::new()));
        let stream = IterStream::with_events(0..3, Rc::clone(&events));

        stream
            .for_each({
                let events = Rc::clone(&events);
                move |item| {
                    events.borrow_mut().push(format!("start {item}"));
                    let events = Rc::clone(&events);
                    async move {
                        runite::yield_now().await;
                        events.borrow_mut().push(format!("done {item}"));
                    }
                }
            })
            .await;

        assert_eq!(
            &*events.borrow(),
            &[
                "poll 0", "start 0", "done 0", "poll 1", "start 1", "done 1", "poll 2", "start 2",
                "done 2", "poll eof",
            ]
        );
    });
}
