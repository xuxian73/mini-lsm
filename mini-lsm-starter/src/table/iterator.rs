#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::{Ok, Result};

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator};

/// An iterator over the contents of an SSTable.
#[derive(Debug)]
pub struct SsTableIterator {
    table: Arc<SsTable>,
    block_iterator: BlockIterator,
    block_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let block = table.read_block(0)?;
        Ok(SsTableIterator {
            table,
            block_iterator: BlockIterator::create_and_seek_to_first(block),
            block_idx: 0,
        })
    }

    /// Seek to the first key-value pair.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let block = self.table.read_block(0)?;
        self.block_iterator = BlockIterator::create_and_seek_to_first(block);
        self.block_idx = 0;
        Ok(())
    }

    fn seek_to_key_inner(table: Arc<SsTable>, key: &[u8]) -> Result<(BlockIterator, usize)> {
        let mut block_idx = table.find_block_idx(key);
        let block = table.read_block(block_idx)?;
        let mut block_iterator = BlockIterator::create_and_seek_to_key(block, key);
        if !block_iterator.is_valid() {
            block_idx += 1;
            if block_idx < table.block_metas.len() {
                let block = table.read_block(block_idx)?;
                block_iterator = BlockIterator::create_and_seek_to_first(block);
            }
        }
        Ok((block_iterator, block_idx))
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: &[u8]) -> Result<Self> {
        let (block_iterator, block_idx) = Self::seek_to_key_inner(table.clone(), key)?;
        Ok(SsTableIterator {
            table,
            block_iterator,
            block_idx,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) -> Result<()> {
        (self.block_iterator, self.block_idx) = Self::seek_to_key_inner(self.table.clone(), key)?;
        println!("block_idx: {}", self.block_idx);
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    fn value(&self) -> &[u8] {
        self.block_iterator.value()
    }

    fn key(&self) -> &[u8] {
        self.block_iterator.key()
    }

    fn is_valid(&self) -> bool {
        self.block_idx < self.table.block_metas.len() && self.block_iterator.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.block_iterator.next();
        if !self.block_iterator.is_valid() {
            self.block_idx += 1;
            if self.block_idx < self.table.num_of_blocks() {
                let block = self.table.read_block(self.block_idx)?;
                self.block_iterator = BlockIterator::create_and_seek_to_first(block);
            }
        }
        Ok(())
    }
}
