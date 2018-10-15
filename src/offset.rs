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

#[derive(Eq, PartialEq, Hash, Copy, Clone, Default, Debug)]
/// Pointer to persistent data. Limited to 2^44
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
            if n > 0xfffffffffffu64 {
                panic!("offset {} greater than 2^44-1", n);
            }
        }

        Offset(n & 0xfffffffffffu64)
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
        self.0 > 0 && self.0 < (1 << 43)
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

    /// compress a vector of offsets assuming that they are in ascending order
    pub fn compress_ascending (offsets: Vec<Offset>) -> Vec<u8> {
        let mut result = Vec::new();
        #[cfg(debug_assertions)]
            {
                if offsets.len() > 255 {
                    panic!("can not compress offset array with length > 255");
                }
            }
        result.write_u8(offsets.len() as u8).unwrap();
        if let Some(first) = offsets.first() {
            let mut prev = first.as_u64();
            result.write_u48::<BigEndian>(prev).unwrap();
            for next in offsets.iter().skip(1) {
                let next = next.as_u64();
                write_diff(next - prev, &mut result);
                prev = next;
            }
        }

        fn write_diff (d: u64, result: &mut Vec<u8>) {
            let s = (d & 0x0f) as u8;
            if d <= 0xf {
                result.push(s);
            } else if d <= 0xfff {
                result.push(s | 0x10);
                result.push((d >> 4) as u8);
            } else if d <= 0xfffff {
                result.push(s | 0x20);
                result.write_u16::<BigEndian>((d >> 4) as u16).unwrap();
            } else if d <= 0xfffffff {
                result.push(s | 0x30);
                result.write_u24::<BigEndian>((d >> 4) as u32).unwrap();
            } else if d <= 0xfffffffff {
                result.push(s | 0x40);
                result.write_u32::<BigEndian>((d >> 4) as u32).unwrap();
            } else if d <= 0xfffffffffff {
                result.push(s | 0x50);
                result.write_u32::<BigEndian>((d >> 4) as u32).unwrap();
                result.write_u8((d >> 36) as u8).unwrap();
            }
        }

        result
    }

    /// decompress a sequence of differences into a vector of offsets in ascending order
    pub fn decompress_ascending(cursor: &mut Cursor<Vec<u8>>) -> Vec<Offset> {
        let mut offsets = Vec::new();
        let n = cursor.read_u8().unwrap() as usize;
        if n > 0 {
            let mut prev  = cursor.read_u48::<BigEndian>().unwrap();
            offsets.push(Offset::from(prev));
            for _ in 0 .. n-1 {
                let next = prev + read_diff(cursor);
                offsets.push(Offset::from(next));
                prev = next;
            }
        }

        fn read_diff(cursor: &mut Cursor<Vec<u8>>) -> u64 {
            let fb = cursor.read_u8().unwrap();
            let s = (fb & 0xf) as u64;
            match fb & 0xf0 {
                0x00 => return s,
                0x10 => {
                    let b = cursor.read_u8().unwrap() as u64;
                    return s + (b << 4);
                },
                0x20 => {
                    let b = cursor.read_u16::<BigEndian>().unwrap() as u64;
                    return s + (b << 4);
                },
                0x30 => {
                    let b = cursor.read_u24::<BigEndian>().unwrap() as u64;
                    return s + (b << 4);
                },
                0x40 => {
                    let b = cursor.read_u32::<BigEndian>().unwrap() as u64;
                    return s + (b << 4);
                },
                0x50 => {
                    let b = cursor.read_u32::<BigEndian>().unwrap() as u64;
                    let c = cursor.read_u8().unwrap() as u64;
                    return s + (b << 4) + (c << 36);
                },
                _ => return 0
            };
        }

        offsets
    }
}
