//
// Copyright 2018 Tamas Blummer
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//!
//! # a block in the blockchain store
//!
//! The block is the unit of read and expansion for the data and key file. A block consists of
//! a payload and a used length less or equal to 4088 it also stores its offset
//!
//! <pre>
//! +------------------------------------+
//! |    | payload                       |
//! +----+-------------------------------+
//! |u48 | block offset                  |
//! +----+-------------------------------+
//! |u16 | used length                   |
//! +----+-------------------------------+
//! </pre>
//!

use error::BCSError;
use offset::Offset;

pub const BLOCK_SIZE: usize = 4096;
pub const PAYLOAD_MAX: usize = 4088;

/// A block of the persistent files
struct Block {
    payload: [u8; PAYLOAD_MAX],
    offset: Offset,
    used: usize
}

impl Block {
    /// create a new empty block to be appended at given offset
    pub fn new (offset: Offset) -> Block {
        Block {payload: [0u8; PAYLOAD_MAX], offset, used: 0}
    }

    /// append some data and return used length
    /// will return Error::DoesNotFit if data does not fit in
    pub fn append (&mut self, data: &[u8]) -> Result<Offset, BCSError> {
        if data.len() > (1 << 15) {
            return Err(BCSError::DoesNotFit);
        }
        let new_len = self.used as usize + data.len();
        if new_len > PAYLOAD_MAX {
            return Err(BCSError::DoesNotFit);
        }
        self.payload [self.used .. new_len].copy_from_slice(data);
        self.used = new_len;
        Ok(Offset::new(new_len).unwrap())
    }

    /// return used length
    pub fn used (&self) -> usize {
        self.used
    }

    pub fn offset(&self) -> Offset {
        self.offset
    }

    /// finish a block after appends to write out
    pub fn finish (&self) -> [u8; BLOCK_SIZE] {
        use std::mem::transmute;

        let mut block = [0u8; BLOCK_SIZE];
        block[0 .. self.used].copy_from_slice (&self.payload[0 .. self.used]);
        let used_bytes: [u8; 2] = unsafe { transmute((self.used as u16).to_be()) };
        block[BLOCK_SIZE - 2 .. BLOCK_SIZE].copy_from_slice(&used_bytes[..]);
        self.offset.serialize(&mut block[BLOCK_SIZE - 8 .. BLOCK_SIZE - 2]);

        block
    }
}

#[cfg(test)]
mod test {
    extern crate hex;

    use super::*;
    #[test]
    fn form_test () {
        let mut block = Block::new(Offset::new(4711).unwrap());
        let payload: &[u8] = "hello world".as_bytes();
        block.append(payload).unwrap();
        let result = block.finish();

        let mut check = [0u8; BLOCK_SIZE];
        check[0 .. payload.len()].copy_from_slice(payload);
        check[BLOCK_SIZE-1] = payload.len() as u8;
        check[BLOCK_SIZE-3] = (4711 % 256) as u8;
        check[BLOCK_SIZE-4] = (4711 / 256) as u8;
        assert_eq!(hex::encode(&result[..]), hex::encode(&check[..]));
    }

    #[test]
    fn append_test () {
        let mut block = Block::new(Offset::new(4711).unwrap());
        assert!(block.append(&[0u8; 5000]).is_err());
        for _ in 0 .. 3 {
            assert!(block.append(&[0u8; 1024]).is_ok());
        }
        let used = block.used();
        assert!(used == 3*1024);
        assert!(block.append(&[0u8; 1024]).is_err());
        assert!(block.append(vec!(0u8; PAYLOAD_MAX - used).as_slice()).is_ok());
    }
}
