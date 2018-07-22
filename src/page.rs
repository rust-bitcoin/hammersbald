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
//! # a page in the blockchain store
//!
//! The page is the unit of read and expansion. A page consists of
//! a payload and a used length less or equal to 4088 it also stores its offset
//!
//! <pre>
//! +------------------------------------+
//! |    | payload                       |
//! +----+-------------------------------+
//! |u48 | block offset                  |
//! +----+-------------------------------+
//! </pre>
//!

use error::BCSError;
use types::Offset;

pub const PAGE_SIZE: usize = 4096;
pub const PAYLOAD_MAX: usize = 4090;

/// A page of the persistent files
#[derive(Clone)]
pub struct Page {
    pub payload: [u8; PAYLOAD_MAX],
    pub offset: Offset
}

impl Page {
    /// create a new empty page to be appended at given offset
    pub fn new (offset: Offset) -> Page {
        Page {payload: [0u8; PAYLOAD_MAX], offset}
    }

    /// create a Page from read buffer
    pub fn from_buf (buf: [u8; PAGE_SIZE]) -> Page {
        let mut payload = [0u8; PAYLOAD_MAX];
        payload.copy_from_slice(&buf[0..PAYLOAD_MAX]);
        Page {payload, offset: Offset::from_slice(&buf[PAYLOAD_MAX .. PAYLOAD_MAX + 6]).unwrap() }
    }

    /// append some data
    /// will return Error::DoesNotFit if data does not fit into the page
    pub fn write (&mut self, pos: usize, data: & [u8]) -> Result<(), BCSError> {
        if pos + data.len() > PAYLOAD_MAX {
            return Err (BCSError::DoesNotFit);
        }
        self.payload [pos .. pos + data.len()].copy_from_slice(&data[..]);
        Ok(())
    }

    /// write an offset
    pub fn write_offset (&mut self, pos: usize, offset: Offset) -> Result<(), BCSError> {
        if pos + 6 > PAYLOAD_MAX {
            return Err (BCSError::DoesNotFit);
        }
        offset.serialize(&mut self.payload[pos .. pos + 6]);
        Ok(())
    }

    /// read some data
    /// will return Error::DoesNotFit if data does not fit into the page
    pub fn read (&self, pos: usize, data: &mut [u8]) -> Result<(), BCSError> {
        if pos + data.len() > PAYLOAD_MAX {
            return Err (BCSError::DoesNotFit);
        }
        let len = data.len();
        data[..].copy_from_slice(&self.payload [pos .. pos + len]);
        Ok(())
    }

    /// read a stored offset
    pub fn read_offset(&self, pos: usize) -> Result<Offset, BCSError> {
        let mut buf = [0u8;6];
        self.read(pos, &mut buf)?;
        Offset::from_slice(&buf)
    }

    /// finish a page after appends to write out
    pub fn finish (&self) -> [u8; PAGE_SIZE] {
        let mut page = [0u8; PAGE_SIZE];
        page[0 .. PAYLOAD_MAX].copy_from_slice (&self.payload[..]);
        self.offset.serialize(&mut page[PAYLOAD_MAX ..]);
        page
    }
}

#[cfg(test)]
mod test {
    extern crate hex;

    use super::*;
    #[test]
    fn form_test () {
        let mut page = Page::new(Offset::new(4711).unwrap());
        let payload: &[u8] = "hello world".as_bytes();
        page.write(0,payload).unwrap();
        let result = page.finish();

        let mut check = [0u8; PAGE_SIZE];
        check[0 .. payload.len()].copy_from_slice(payload);
        check[PAGE_SIZE -1] = (4711 % 256) as u8;
        check[PAGE_SIZE -2] = (4711 / 256) as u8;
        assert_eq!(hex::encode(&result[..]), hex::encode(&check[..]));

        let page2 = Page::from_buf(check);
        assert_eq!(page.offset, page2.offset);
        assert_eq!(hex::encode(&page.payload[..]), hex::encode(&page2.payload[..]));
    }
}
