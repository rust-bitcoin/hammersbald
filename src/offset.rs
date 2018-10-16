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

use page::PAGE_SIZE;

use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

use std::io::Cursor;
use std::cmp::Ordering;
use std::fmt;
use std::ops;

#[derive(Eq, PartialEq, Hash, Copy, Clone, Default, Debug)]
/// Pointer to persistent data. Limited to 2^48
pub struct Offset(u64);

impl Ord for Offset {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for Offset {
    fn partial_cmp(&self, other: &Offset) -> Option<Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl From<u64> for Offset {
    fn from(n: u64) -> Self {
        #[cfg(debug_assertions)]
        {
            if n > 0xffffffffffffu64 {
                panic!("offset {} greater than 2^48-1", n);
            }
        }

        Offset(n & 0xffffffffffffu64)
    }
}

impl<'a> From<&'a [u8]> for Offset {
    fn from(slice: &'a [u8]) -> Self {
        Offset::from(Cursor::new(slice).read_u48::<BigEndian>().unwrap())
    }
}

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.0)
    }
}

/// can read offsets from this
pub trait OffsetReader {
    /// read offset
    fn read_offset (&mut self) -> Offset;
}

impl OffsetReader for Cursor<Vec<u8>> {
    fn read_offset(&mut self) -> Offset {
        Offset(self.read_u48::<BigEndian>().unwrap())
    }
}

impl ops::Add<u64> for Offset {
    type Output = Offset;

    fn add(self, rhs: u64) -> <Self as ops::Add<u64>>::Output {
        Offset::from(self.as_u64() + rhs)
    }
}

impl ops::AddAssign<u64> for Offset {
    fn add_assign(&mut self, rhs: u64) {
        #[cfg(debug_assertions)]
        {
            if self.0 + rhs > 0xffffffffffffu64 {
                panic!("offset would become greater than 2^48-1");
            }
        }
        self.0 += rhs;
    }
}

impl ops::Sub<u64> for Offset {
    type Output = Offset;

    fn sub(self, rhs: u64) -> <Self as ops::Sub<u64>>::Output {
        Offset::from(self.as_u64() - rhs)
    }
}

impl ops::SubAssign<u64> for Offset {
    fn sub_assign(&mut self, rhs: u64) {
        #[cfg(debug_assertions)]
        {
            if rhs >= self.0 {
                panic!("offset would become invalid through subtraction");
            }
        }
        self.0 -= rhs;
    }
}

impl Offset {
    /// serialize to a vector of bytes
    pub fn to_vec(&self) -> Vec<u8> {
        let mut v = Vec::new();
        v.write_u48::<BigEndian>(self.0).unwrap();
        v
    }

    /// construct an invalid offset
    pub fn invalid () -> Offset {
        Offset::from(0)
    }

    /// is this a valid offset?
    pub fn is_valid (&self) -> bool {
        self.0 > 0 && self.0 < (1 << 47)
    }

    /// convert to a number
    pub fn as_u64 (&self) -> u64 {
        return self.0;
    }


    /// offset of the page of this offset
    pub fn this_page(&self) -> Offset {
        Offset::from((self.0/ PAGE_SIZE as u64)* PAGE_SIZE as u64)
    }

    /// compute page number of an offset
    pub fn page_number(&self) -> u64 {
        self.0/PAGE_SIZE as u64
    }

    /// position within the offset's page
    pub fn in_page_pos(&self) -> usize {
        (self.0 % PAGE_SIZE as u64) as usize
    }
}
