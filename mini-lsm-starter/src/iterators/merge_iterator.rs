use std::cmp::{self};
use std::collections::BinaryHeap;

use anyhow::Result;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        if !self.1.is_valid() {
            return Some(cmp::Ordering::Less);
        }
        if !other.1.is_valid() {
            return Some(cmp::Ordering::Greater);
        }
        match self.1.key().cmp(other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, perfer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        if iters.is_empty() {
            return Self {
                iters: BinaryHeap::new(),
                current: None,
            };
        }
        let mut heap = BinaryHeap::new();
        for (i, iter) in iters.into_iter().enumerate() {
            heap.push(HeapWrapper(i, iter));
        }
        let current = heap.pop();
        Self {
            iters: heap,
            current,
        }
    }
}

impl<I: StorageIterator> StorageIterator for MergeIterator<I> {
    fn key(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(|x| x.1.is_valid())
            .unwrap_or(false)
    }

    fn next(&mut self) -> Result<()> {
        if !self.current.as_ref().unwrap().1.is_valid() {
            return Ok(());
        }
        while let Some(mut iter) = self.iters.peek_mut() {
            if !iter.1.is_valid() {
                if self.current.as_mut().unwrap().1.next().is_err() {}
                return Ok(());
            }
            if iter.1.key() <= self.current.as_ref().unwrap().1.key() {
                if iter.1.next().is_err() {}
            } else {
                break;
            }
        }
        if self.current.as_mut().unwrap().1.next().is_err() {
            while !self.current.as_ref().unwrap().1.is_valid() && !self.iters.is_empty() {
                self.current = self.iters.pop();
            }
        }

        if !self.iters.is_empty() && self.current.as_ref().unwrap() < self.iters.peek().unwrap() {
            std::mem::swap(
                self.current.as_mut().unwrap(),
                &mut *self.iters.peek_mut().unwrap(),
            );
        }
        Ok(())
    }
}
