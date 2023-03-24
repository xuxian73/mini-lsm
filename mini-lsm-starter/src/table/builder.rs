use std::path::Path;
use std::sync::Arc;

use anyhow::{Ok, Result};
use bytes::{BufMut, Bytes};

use super::{BlockMeta, FileObject, SsTable};
use crate::{
    block::{BlockBuilder, BlockIterator},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    pub(super) meta: Vec<BlockMeta>,
    // Add other fields you need.
    pub block_builder: Option<Box<BlockBuilder>>,
    pub blocks: Vec<u8>,
    pub block_size: usize,
    pub size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            meta: vec![],
            block_builder: Some(Box::new(BlockBuilder::new(block_size))),
            blocks: vec![],
            block_size,
            size: 0,
        }
    }

    pub fn build_block(&mut self) {
        let block = self.block_builder.take().unwrap().build();
        let offset = self.blocks.len();
        self.blocks.extend(block.encode());
        self.meta.push(BlockMeta {
            offset,
            first_key: Bytes::copy_from_slice(
                BlockIterator::create_and_seek_to_first(Arc::new(block)).key(),
            ),
        });
        self.block_builder = Some(Box::new(BlockBuilder::new(self.block_size)));
    }

    /// Adds a key-value pair to SSTable
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        while !self.block_builder.as_mut().unwrap().add(key, value) {
            self.build_block();
        }
    }

    /// Get the estimated size of the SSTable.
    pub fn estimated_size(&self) -> usize {
        self.block_size * self.blocks.len()
    }

    /// Builds the SSTable and writes it to the given path. No need to actually write to disk until
    /// chapter 4 block cache.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        if !self.block_builder.as_ref().unwrap().is_empty() {
            self.build_block();
        }
        let block_meta_offset = self.blocks.len();
        BlockMeta::encode_block_meta(&self.meta, &mut self.blocks);
        self.blocks.put_u64(block_meta_offset as u64);
        Ok(SsTable {
            file: FileObject::create(path.as_ref(), self.blocks).unwrap(),
            block_metas: self.meta,
            block_meta_offset,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
