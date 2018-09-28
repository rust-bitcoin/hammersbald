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
//! # Types used in db files
//! Offset an unsigned 48 bit integer used as file offset
//! U24 an unsigned 24 bit integer for data element sizes

use error::BCDBError;
use page::PAGE_SIZE;

use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

use std::io::Cursor;

#[derive(Eq, PartialEq, Hash, Copy, Clone, Default, Debug)]
/// Pointer to persistent data. Limited to 2^48
pub struct Offset(u64);

impl From<u64> for Offset {
    fn from(n: u64) -> Self {
        Offset(n & 0xffffffffffffu64)
    }
}

impl<'a> From<&'a [u8]> for Offset {
    fn from(slice: &'a [u8]) -> Self {
        Offset::from(Cursor::new(slice).read_u48::<BigEndian>().unwrap())
    }
}

impl Offset {

    /// read from serialized bytes
    pub fn read_vec(cursor: &mut Cursor<Vec<u8>>) -> Result<Offset, BCDBError> {
        Ok(Offset(cursor.read_u48::<BigEndian>()?))
    }

    /// serialize to a vector of bytes
    pub fn to_vec(&self) -> Vec<u8> {
        let mut v = Vec::new();
        v.write_u48::<BigEndian>(self.0).unwrap();
        v
    }

    /// convert to a number
    pub fn as_u64 (&self) -> u64 {
        return self.0;
    }


    /// offset of the page of this offset
    pub fn this_page(&self) -> Offset {
        Offset::from((self.0/ PAGE_SIZE as u64)* PAGE_SIZE as u64)
    }

    /// page offset after this offset
    pub fn next_page(&self) -> Offset {
        Offset::from((self.0/ PAGE_SIZE as u64 + 1)* PAGE_SIZE as u64)
    }

    /// compute page number of an offset
    pub fn page_number(&self) -> u64 {
        self.0/PAGE_SIZE as u64
    }

    /// position within the offset's page
    pub fn in_page_pos(&self) -> usize {
        (self.0 - (self.0/ PAGE_SIZE as u64)* PAGE_SIZE as u64) as usize
    }
}

#[derive(Eq, PartialEq, Hash, Copy, Clone, Default, Debug)]
pub(crate) struct U24 (usize);

impl From<usize> for U24 {
    fn from(n: usize) -> Self {
        U24(n & 0xffffffusize)
    }
}

impl<'a> From<&'a [u8]> for U24 {
    fn from(slice: &'a [u8]) -> Self {
        U24::from(Cursor::new(slice).read_u24::<BigEndian>().unwrap() as usize)
    }
}

impl U24 {

    pub fn new (value: usize) ->Result<U24, BCDBError> {
        if value > 1 << 23 {
            return Err(BCDBError::InvalidOffset);
        }
        Ok(U24(value))
    }

    pub fn from_slice (slice: &[u8]) -> Result<U24, BCDBError> {
        if slice.len() != 3 {
            return Err(BCDBError::InvalidOffset);
        }
        Ok(U24(Cursor::new(slice).read_u24::<BigEndian>()? as usize))
    }

    pub fn as_usize (&self) -> usize {
        return self.0;
    }

    pub fn serialize (&self, mut into: &mut [u8]) {
        into.write_u24::<BigEndian>(self.0 as u32).unwrap();
    }
}

