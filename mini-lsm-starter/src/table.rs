#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

use std::path::Path;
use std::sync::Arc;

use anyhow::{Ok, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::lsm_storage::BlockCache;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: Bytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
    ) {
        // write block_meta to buf
        for meta in block_meta {
            buf.put_u64(meta.offset as u64);
            buf.put_u64(meta.first_key.len() as u64);
            buf.put_slice(&meta.first_key);
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(buf: impl Buf) -> Vec<BlockMeta> {
        // read block_meta from buf
        let mut block_meta = vec![];
        let mut buf = buf;
        while buf.has_remaining() {
            let offset = buf.get_u64() as usize;
            let first_key_len = buf.get_u64() as usize;
            let first_key = buf.copy_to_bytes(first_key_len);
            block_meta.push(BlockMeta { offset, first_key });
        }
        block_meta
    }
}

/// A file object.
#[derive(Debug)]
pub struct FileObject(Bytes);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        Ok(self.0[offset as usize..(offset + len) as usize].to_vec())
    }

    pub fn size(&self) -> u64 {
        self.0.len() as u64
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        // Create a new file for path
        // let file = match fs::File::create(path) {
        //     Ok(file) => file,
        //     Err(e) => return Err(e.into()),
        // };
        // Write data to file
        // match file.write_all_at(&data, 0) {
        //     Ok(_) => Ok(FileObject(Bytes::new())),
        //     Err(e) => Err(e.into()),
        // }
        Ok(Self(data.into()))
    }

    pub fn open(path: &Path) -> Result<Self> {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct SsTable {
    file: FileObject,
    block_metas: Vec<BlockMeta>,
    block_meta_offset: usize,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        // read from FileObject
        let block_meta_offset = file.read(file.size() - 8, 8)?;
        // convert block_meta_offset into u64
        let block_meta_offset = bytes::Buf::get_u64(&mut block_meta_offset.as_slice());
        // read block_meta from file
        let block_meta = file.read(block_meta_offset, file.size() - 8 - block_meta_offset)?;
        // decode block_meta
        let block_meta = BlockMeta::decode_block_meta(block_meta.as_slice());
        Ok(Self {
            file,
            block_metas: block_meta,
            block_meta_offset: block_meta_offset as usize,
        })
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        if block_idx >= self.block_metas.len() {
            return Err(anyhow::anyhow!("block_idx out of range"));
        }
        let offset = self.block_metas[block_idx].offset as u64;
        let end = match self.block_metas.len() > block_idx + 1 {
            true => self.block_metas[block_idx + 1].offset,
            false => self.block_meta_offset,
        } as u64;
        let block_data = self.file.read(offset, end - offset)?;
        Ok(Arc::new(Block::decode(block_data.as_slice())))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        unimplemented!()
    }

    /// Find the block that may contain `key`.
    pub fn find_block_idx(&self, key: &[u8]) -> usize {
        self.block_metas
            .partition_point(|meta| meta.first_key < key)
            .saturating_sub(1)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_metas.len()
    }
}

#[cfg(test)]
mod tests;
