#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    block: Arc<Block>,
    key: Vec<u8>,
    value: Vec<u8>,
    idx: usize,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: Vec::new(),
            value: Vec::new(),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iterator = BlockIterator::new(block);
        iterator.seek_to_first();
        iterator
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: &[u8]) -> Self {
        let mut iterator = BlockIterator::new(block);
        iterator.seek_to_key(key);
        iterator
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Returns true if the iterator is valid.
    pub fn is_valid(&self) -> bool {
        self.idx < self.block.offsets.len()
    }

    /// Seeks to the idx-th entry.
    /// Used by `seek_to_first` and `next`.
    fn seek_idx(&mut self) {
        let mut offset = self.block.offsets[self.idx];
        // get key len
        let key_len = u16::from_be_bytes([
            self.block.data[offset as usize],
            self.block.data[(offset + 1) as usize],
        ]);
        offset += 2;
        self.key = self.block.data[offset as usize..(offset + key_len) as usize].to_vec();
        // get value len
        offset += key_len;
        let value_len = u16::from_be_bytes([
            self.block.data[(offset) as usize],
            self.block.data[(offset + 1) as usize],
        ]);
        offset += 2;
        self.value = self.block.data[(offset) as usize..(offset + value_len) as usize].to_vec();
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.idx = 0;
        if self.is_valid() {
            self.seek_idx();
        }
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        if self.is_valid() {
            self.seek_idx();
        }
    }

    /// Seek to the first key that >= `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) {
        self.seek_to_first();
        while self.is_valid() {
            if self.key() < key {
                self.next();
            } else {
                break;
            }
        }
    }
}
