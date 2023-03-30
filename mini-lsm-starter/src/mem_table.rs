use std::ops::Bound;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::table::SsTableBuilder;

/// A basic mem-table based on crossbeam-skiplist
pub struct MemTable {
    map: Arc<SkipMap<Bytes, Bytes>>,
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create() -> Self {
        MemTable {
            map: Arc::new(SkipMap::new()),
        }
    }

    /// Get a value by key.
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        let kv = self.map.get(&Bytes::copy_from_slice(key));
        match kv.is_none() {
            true => None,
            false => Some(kv.unwrap().value().clone()),
        }
    }

    /// Put a key-value pair into the mem-table.
    pub fn put(&self, key: &[u8], value: &[u8]) {
        self.map
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    /// turn a &[u8] bound into a Bytes bound
    fn bound_to_bytes(bound: Bound<&[u8]>) -> Bound<Bytes> {
        match bound {
            Bound::Included(k) => Bound::Included(Bytes::copy_from_slice(k)),
            Bound::Excluded(k) => Bound::Excluded(Bytes::copy_from_slice(k)),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> MemTableIterator {
        let lower = MemTable::bound_to_bytes(lower);
        let upper = MemTable::bound_to_bytes(upper);
        let mut iter = MemTableIteratorBuilder {
            map: Arc::clone(&self.map),
            iter_builder: |map| map.range((lower, upper)),
            item: (Bytes::from_static(&[]), Bytes::from_static(&[])),
        }
        .build();
        // get the first item of the iterator
        let entry = iter.with_iter_mut(|iter| {
            iter.next()
                .map(|x| (x.key().clone(), x.value().clone()))
                .unwrap_or_else(|| (Bytes::from_static(&[]), Bytes::from_static(&[])))
        });
        iter.with_mut(|x| *x.item = entry);
        iter
    }

    /// Flush the mem-table to SSTable.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        for entry in self.map.iter() {
            builder.add(entry.key(), entry.value());
        }
        Ok(())
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

/// An iterator over a range of `SkipMap`.
#[self_referencing]
pub struct MemTableIterator {
    map: Arc<SkipMap<Bytes, Bytes>>,
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    item: (Bytes, Bytes),
}

impl StorageIterator for MemTableIterator {
    fn value(&self) -> &[u8] {
        self.borrow_item().1.as_ref()
    }

    fn key(&self) -> &[u8] {
        self.borrow_item().0.as_ref()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let entry = self.with_iter_mut(|iter| {
            iter.next()
                .map(|x| (x.key().clone(), x.value().clone()))
                .unwrap_or_else(|| (Bytes::from_static(&[]), Bytes::from_static(&[])))
        });
        self.with_mut(|x| *x.item = entry);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
