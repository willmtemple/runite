//! Timer heap shared by both platform runtimes.

use std::collections::HashMap;
use std::time::Duration;

use super::LocalTask;

/// Tag identifying the kind of timer at the heap level.
///
/// Both platforms use the Linux side-table model: interval callbacks live in
/// `ThreadState::live_intervals` keyed by `TimerNode::id`, so `Interval` carries
/// no payload. This keeps the heap free of `Rc`/`RefCell` cycles and lets
/// `clear_interval` work uniformly whether the next tick is parked in the heap
/// or pending as a macrotask.
pub(crate) enum TimerKind {
    Timeout(LocalTask),
    Interval,
}

pub(crate) struct TimerNode {
    pub(crate) id: usize,
    pub(crate) deadline: Duration,
    pub(crate) kind: TimerKind,
}

impl TimerNode {
    pub(crate) fn timeout(id: usize, deadline: Duration, callback: LocalTask) -> Self {
        Self {
            id,
            deadline,
            kind: TimerKind::Timeout(callback),
        }
    }

    pub(crate) fn interval(id: usize, deadline: Duration) -> Self {
        Self {
            id,
            deadline,
            kind: TimerKind::Interval,
        }
    }
}

/// Min-heap keyed by `(deadline, id)` with O(log n) random removal via a
/// secondary `id → heap-index` map. Random removal is needed because timeout
/// and interval cancellation can target any node in the heap.
pub(crate) struct TimerHeap {
    nodes: Vec<TimerNode>,
    positions: HashMap<usize, usize>,
}

impl TimerHeap {
    pub(crate) fn new() -> Self {
        Self {
            nodes: Vec::new(),
            positions: HashMap::new(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub(crate) fn peek_deadline(&self) -> Option<Duration> {
        self.nodes.first().map(|node| node.deadline)
    }

    pub(crate) fn insert(&mut self, node: TimerNode) {
        let index = self.nodes.len();
        self.positions.insert(node.id, index);
        self.nodes.push(node);
        self.sift_up(index);
    }

    pub(crate) fn remove(&mut self, id: usize) -> Option<TimerNode> {
        let index = *self.positions.get(&id)?;
        self.positions.remove(&id);

        let last = self.nodes.pop().expect("heap index should be valid");
        if index == self.nodes.len() {
            return Some(last);
        }

        let removed = std::mem::replace(&mut self.nodes[index], last);
        self.positions.insert(self.nodes[index].id, index);
        self.fix(index);
        Some(removed)
    }

    fn pop_min(&mut self) -> Option<TimerNode> {
        let id = self.nodes.first()?.id;
        self.remove(id)
    }

    pub(crate) fn pop_due(&mut self, now: Duration) -> Vec<TimerNode> {
        let mut due = Vec::new();
        while self.peek_deadline().is_some_and(|deadline| deadline <= now) {
            due.push(self.pop_min().expect("timer heap should contain a minimum"));
        }
        due
    }

    fn fix(&mut self, index: usize) {
        if index > 0 && self.less(index, parent(index)) {
            self.sift_up(index);
        } else {
            self.sift_down(index);
        }
    }

    fn sift_up(&mut self, mut index: usize) {
        while index > 0 {
            let parent = parent(index);
            if !self.less(index, parent) {
                break;
            }
            self.swap(index, parent);
            index = parent;
        }
    }

    fn sift_down(&mut self, mut index: usize) {
        loop {
            let left = index * 2 + 1;
            let right = left + 1;
            let mut smallest = index;

            if left < self.nodes.len() && self.less(left, smallest) {
                smallest = left;
            }
            if right < self.nodes.len() && self.less(right, smallest) {
                smallest = right;
            }
            if smallest == index {
                break;
            }

            self.swap(index, smallest);
            index = smallest;
        }
    }

    fn less(&self, lhs: usize, rhs: usize) -> bool {
        let left = &self.nodes[lhs];
        let right = &self.nodes[rhs];
        (left.deadline, left.id) < (right.deadline, right.id)
    }

    fn swap(&mut self, lhs: usize, rhs: usize) {
        self.nodes.swap(lhs, rhs);
        self.positions.insert(self.nodes[lhs].id, lhs);
        self.positions.insert(self.nodes[rhs].id, rhs);
    }
}

const fn parent(index: usize) -> usize {
    (index - 1) / 2
}
