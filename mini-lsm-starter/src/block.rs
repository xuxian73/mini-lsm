#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted
/// key-value pairs.
#[derive(Debug)]
pub struct Block {
    data: Vec<u8>,
    offsets: Vec<u16>,
}

impl Block {
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.data.len() + self.offsets.len() * 2);
        // put num of element
        buf.put_u16(self.offsets.len() as u16);
        // put offsets into buffer
        for offset in self.offsets.iter() {
            buf.put_u16(*offset);
        }
        // put data into buffer
        buf.put_slice(&self.data);

        buf.freeze()
    }

    pub fn decode(data: &[u8]) -> Self {
        let mut buf = BytesMut::from(data);
        // read num of element from last two byte of buf
        let mut num_of_elements = buf.split_to(2);
        let num_of_elements = num_of_elements.get_u16() as usize;
        // read offsets from last two byte of buf
        let mut offsets_buf = buf.split_to(num_of_elements * 2);
        let mut offsets = Vec::with_capacity(num_of_elements);
        for _ in 0..num_of_elements {
            offsets.push(offsets_buf.get_u16());
        }
        // read data from buf
        Self {
            data: buf.to_vec(),
            offsets,
        }
    }
}

#[cfg(test)]
mod tests;
