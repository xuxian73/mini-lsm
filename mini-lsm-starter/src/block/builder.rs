use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    capacity: usize,
    size: usize,
    data: Vec<u8>,
    offsets: Vec<u16>,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            capacity: block_size,
            size: 0,
            data: Vec::new(),
            offsets: Vec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        // check if able to add new key-value pair
        if self.size + key.len() + value.len() + 6 > self.capacity {
            return false;
        }
        // add key-value pair to block
        self.offsets.push(self.data.len() as u16);
        let key_len = key.len() as u16;
        let value_len = value.len() as u16;
        self.data.extend(key_len.to_be_bytes());
        self.data.extend_from_slice(key);
        self.data.extend(value_len.to_be_bytes());
        self.data.extend_from_slice(value);
        self.size += key.len() + value.len() + 6;
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
