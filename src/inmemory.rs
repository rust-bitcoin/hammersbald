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
//! # in memory store for tests
//!
//! Implements in-memory Read and Write for tests

use error::BCSError;
use bcdb::RW;
use logfile::LogFile;
use bcdb::{BCDBFactory, BCDB};
use keyfile::KeyFile;
use datafile::DataFile;

use std::io::Read;
use std::io::Write;
use std::io::Seek;
use std::io::SeekFrom;
use std::io;
use std::cmp::min;
use std::sync::{Mutex,Arc};

/// in memory representation of a file
pub struct InMemory {
    data: Vec<u8>,
    pos: usize,
    append: bool
}

impl InMemory {
    /// create a new file
    pub fn new (append: bool) -> InMemory {
        InMemory{data: Vec::new(), pos: 0, append}
    }
}

impl BCDBFactory for InMemory {
    fn new_db (_name: &str) -> Result<BCDB, BCSError> {
        let log = Arc::new(Mutex::new(LogFile::new(Box::new(InMemory::new(true)))));
        let table = KeyFile::new(Box::new(InMemory::new(false)), log);
        let data = DataFile::new(Box::new(InMemory::new(true)));

        BCDB::new(table, data)
    }
}

impl RW for InMemory {
    fn len(&mut self) -> Result<usize, BCSError> {
        Ok(self.data.len())
    }

    fn truncate(&mut self, len: usize) -> Result<(), BCSError> {
        self.data.truncate(len);
        Ok(())
    }

    fn sync(&self) -> Result<(), BCSError> { Ok(()) }
}

impl Read for InMemory {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        let buflen = buf.len();
        if self.pos + buflen > self.data.len () {
            return Err(io::Error::from(io::ErrorKind::NotFound));
        }
        buf.copy_from_slice(&self.data.as_slice()[self.pos .. self.pos + buflen]);
        self.pos += buflen;
        Ok(buflen)
    }
}

impl Write for InMemory {
    fn write(&mut self, buf: &[u8]) -> Result<usize, io::Error> {
        let buflen = buf.len();
        if self.append {
            self.data.extend_from_slice(buf);
        }
        else {
            let len = self.data.len();
            let pos = self.pos;
            let have = min(buflen, len - pos);
            self.data.as_mut_slice()[pos..pos + have].copy_from_slice(&buf[0..have]);
            if buflen > have {
                self.data.extend_from_slice(&buf[have..buflen]);
            }
        }
        self.pos += buflen;
        Ok(self.pos)
    }

    fn flush(&mut self) -> Result<(), io::Error> {
        Ok(())
    }
}

impl Seek for InMemory {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64, io::Error> {
        match pos {
            SeekFrom::Start(o) => {
                if o > self.data.len() as u64 {
                    return Err(io::Error::from(io::ErrorKind::UnexpectedEof))
                }
                self.pos = o as usize;
            }
            SeekFrom::Current(o) => {
                let newpos = o + self.pos as i64;
                if newpos < 0 || newpos > self.data.len () as i64 {
                    return Err(io::Error::from(io::ErrorKind::UnexpectedEof))
                }
                self.pos = newpos as usize;
            }
            SeekFrom::End(o) => {
                let newpos = o + self.data.len() as i64;
                if newpos < 0 || newpos > self.data.len () as i64 {
                    return Err(io::Error::from(io::ErrorKind::UnexpectedEof))
                }
                self.pos = newpos as usize;
            }
        }
        Ok(self.pos as u64)
    }
}

#[cfg(test)]
mod test {
    extern crate hex;

    use super::*;

    #[test]
    fn in_memory_test() {
        let mut mem = InMemory::new(false);
        assert!(mem.seek(SeekFrom::Start(0)).is_ok());
        assert!(mem.write("Hello".as_bytes()).is_ok());
        assert!(mem.seek(SeekFrom::Start(1)).is_ok());
        let mut buf = [0u8;4];
        assert!(mem.read(&mut buf[..]).is_ok());
        assert_eq! (String::from_utf8(buf.to_vec()).unwrap(), "ello".to_owned());
        assert!(mem.write(" world ".as_bytes()).is_ok());
        assert_eq!(mem.pos, 12);
        assert!(mem.seek(SeekFrom::End(-1)).is_ok());
        assert!(mem.write("!".as_bytes()).is_ok());
        assert_eq!(String::from_utf8(mem.data.clone()).unwrap(), "Hello world!");
        assert_eq!(mem.pos, 12);
        assert!(mem.seek(SeekFrom::Start(12)).is_ok());
        assert!(mem.seek(SeekFrom::Start(13)).is_err());
        assert!(mem.seek(SeekFrom::End(-12)).is_ok());
        assert!(mem.seek(SeekFrom::End(-13)).is_err());
        assert!(mem.seek(SeekFrom::End(-2)).is_ok());
        assert!(mem.seek(SeekFrom::Current(2)).is_ok());
        assert!(mem.seek(SeekFrom::Current(3)).is_err());
        assert!(mem.seek(SeekFrom::Current(-12)).is_ok());
        assert!(mem.seek(SeekFrom::Current(-13)).is_err());
        assert!(mem.seek(SeekFrom::Current(0)).is_ok());
        assert!(mem.seek(SeekFrom::Current(12)).is_ok());
        assert!(mem.write("!".as_bytes()).is_ok());
        assert_eq!(String::from_utf8(mem.data.clone()).unwrap(), "Hello world!!");
        assert!(mem.truncate(10).is_ok());
        assert_eq!(mem.len().unwrap(), 10);
    }
}