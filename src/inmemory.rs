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

use error::BCDBError;
use logfile::LogFile;
use bcdb::{BCDBFactory, BCDB};
use keyfile::KeyFile;
use datafile::DataFile;
use types::Offset;
use page::{PageFile,Page,PAGE_SIZE};

use std::io::Read;
use std::io::Write;
use std::io::Seek;
use std::io::SeekFrom;
use std::io;
use std::cmp::min;
use std::sync::{Mutex,Arc};

/// in memory representation of a file
pub struct InMemory {
    inner: Mutex<Inner>
}

struct Inner {
    data: Vec<u8>,
    pos: usize,
    append: bool
}

impl InMemory {
    /// create a new file
    pub fn new (append: bool) -> InMemory {
        InMemory{inner: Mutex::new(Inner{data: Vec::new(), pos: 0, append})}
    }
}

impl BCDBFactory for InMemory {
    fn new_db (_name: &str) -> Result<BCDB, BCDBError> {
        let log = Arc::new(Mutex::new(LogFile::new(Box::new(InMemory::new(true)))));
        let table = KeyFile::new(Box::new(InMemory::new(false)), log);
        let data = DataFile::new(Box::new(InMemory::new(true)))?;
        let bucket = DataFile::new(Box::new(InMemory::new(true)))?;

        BCDB::new(table, data, bucket)
    }
}

impl PageFile for InMemory {
    fn flush(&mut self) -> Result<(), BCDBError> {Ok(())}

    fn len(&self) -> Result<u64, BCDBError> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.data.len() as u64)
    }

    fn truncate(&mut self, len: u64) -> Result<(), BCDBError> {
        let mut inner = self.inner.lock().unwrap();
        inner.data.truncate(len as usize);
        Ok(())
    }

    fn sync(&self) -> Result<(), BCDBError> { Ok(()) }

    fn read_page (&self, offset: Offset) -> Result<Page, BCDBError> {
        let mut inner = self.inner.lock().unwrap();
        let mut buffer = [0u8; PAGE_SIZE];
        let len = inner.seek(SeekFrom::End(0))?;
        if offset.as_u64() >= len {
            return Err(BCDBError::InvalidOffset);
        }
        inner.seek(SeekFrom::Start(offset.as_u64()))?;
        inner.read(&mut buffer)?;
        let page = Page::from_buf(buffer);
        Ok(page)
    }

    fn append_page(&mut self, page: Page) -> Result<(), BCDBError> {
        let mut inner = self.inner.lock().unwrap();
        inner.write(&page.finish()[..])?;
        Ok(())
    }

    fn write_page(&mut self, page: Page) -> Result<(), BCDBError> {
        let mut inner = self.inner.lock().unwrap();
        inner.seek(SeekFrom::Start(page.offset.as_u64()))?;
        inner.write(&page.finish()[..])?;
        Ok(())
    }

    fn write_batch(&mut self, writes: Vec<Arc<Page>>) -> Result<(), BCDBError> {
        for page in writes {
            use std::ops::Deref;
            self.write_page(page.deref().clone())?;
        }
        Ok(())
    }
}

impl Read for Inner {
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

impl Write for Inner {
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

impl Seek for Inner {
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
