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
//! |u16 | used length                   |
//! +----+-------------------------------+
//! </pre>
//!

use error::BCSError;
use types::Offset;
use std::mem::transmute;
use std::cmp::min;

pub const PAGE_SIZE: usize = 4096;
pub const PAYLOAD_MAX: usize = 4088;

/// A page of the persistent files
#[derive(Clone)]
pub struct Page {
    pub payload: [u8; PAYLOAD_MAX],
    pub offset: Offset,
    pub pos: usize
}

impl Page {
    /// create a new empty page to be appended at given offset
    pub fn new (offset: Offset) -> Page {
        Page {payload: [0u8; PAYLOAD_MAX], offset, pos: 0}
    }

    /// create a Page from read buffer
    pub fn from_buf (buf: [u8; PAGE_SIZE]) -> Result<Page, BCSError> {
        let mut payload = [0u8; PAYLOAD_MAX];
        payload.copy_from_slice(&buf[0..PAYLOAD_MAX]);
        let mut stored_used = [0u8;2];
        stored_used[..].copy_from_slice (&buf[PAYLOAD_MAX+6 .. PAYLOAD_MAX+8]);
        let used_be :u16 = unsafe {transmute(stored_used)};
        let used = u16::from_be(used_be) as usize;
        if used > PAYLOAD_MAX {
            return Err(BCSError::DoesNotFit);
        }
        Ok(Page {payload, offset: Offset::from_slice(&buf[PAYLOAD_MAX .. PAYLOAD_MAX + 6]).unwrap(), pos: used })
    }

    /// append some data and return used length
    /// will return Error::DoesNotFit if data does not fit in
    pub fn append<'a> (&mut self, data: &'a [u8]) -> Result<(Offset, &'a [u8]), BCSError> {
        let have = min(PAYLOAD_MAX - self.pos, data.len());
        self.payload [self.pos .. self.pos + have].copy_from_slice(&data[0 .. have]);
        self.pos += have;
        Ok((Offset::new(self.offset.as_usize() + self.pos)?, &data[have..]))
    }

    /// read from a page starting from given pos
    /// return the number of bytes successfully read into data
    pub fn read(&self, pos: usize, data: &mut [u8]) -> Result<usize, BCSError> {
        if pos > PAYLOAD_MAX {
            return Ok(0)
        }
        let have = min(PAYLOAD_MAX - pos, data.len());
        data[0 .. have].copy_from_slice(&self.payload[pos.. pos + have]);
        Ok(have)
    }

    /// finish a page after appends to write out
    pub fn finish (&self) -> [u8; PAGE_SIZE] {
        let mut page = [0u8; PAGE_SIZE];
        page[0 .. self.pos].copy_from_slice (&self.payload[0 .. self.pos]);
        let used_bytes: [u8; 2] = unsafe { transmute((self.pos as u16).to_be()) };
        page[PAGE_SIZE - 2 ..PAGE_SIZE].copy_from_slice(&used_bytes[..]);
        self.offset.serialize(&mut page[PAGE_SIZE - 8 .. PAGE_SIZE - 2]);

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
        page.append(payload).unwrap();
        let result = page.finish();

        let mut check = [0u8; PAGE_SIZE];
        check[0 .. payload.len()].copy_from_slice(payload);
        check[PAGE_SIZE -1] = payload.len() as u8;
        check[PAGE_SIZE -3] = (4711 % 256) as u8;
        check[PAGE_SIZE -4] = (4711 / 256) as u8;
        assert_eq!(hex::encode(&result[..]), hex::encode(&check[..]));

        let page2 = Page::from_buf(check).unwrap();
        assert_eq!(page.pos, page2.pos);
        assert_eq!(page.offset, page2.offset);
        assert_eq!(hex::encode(&page.payload[..]), hex::encode(&page2.payload[..]));
    }

    #[test]
    fn append_test () {
        let mut page = Page::new(Offset::new(4711).unwrap());
        for _ in 0 .. 3 {
            assert!(page.append(&[0u8; 1024]).is_ok());
        }
        let used = page.pos;
        assert!(used == 3*1024);
    }

    #[test]
    fn fit_test () {
        let mut page = Page::new(Offset::new(4711).unwrap());
        assert!(page.append(&[0u8;4000]).is_ok());
        assert_eq!(page.append(&[0u8;100]).unwrap().1.len(), 4100 - PAYLOAD_MAX);
    }
}
