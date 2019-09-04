//
// Copyright 2018-2019 Tamas Blummer
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
//! # transient store for tests
//!
//! Implements in-memory Read and Write for tests

use error::Error;
use logfile::LogFile;
use api::{Hammersbald, HammersbaldAPI};
use tablefile::TableFile;
use datafile::DataFile;
use pref::PRef;
use page::{Page,PAGE_SIZE};
use pagedfile::PagedFile;
use asyncfile::AsyncFile;
use cachedfile::CachedFile;

use std::io::Read;
use std::io::Write;
use std::io::Seek;
use std::io::SeekFrom;
use std::io;
use std::cmp::min;
use std::sync::Mutex;

/// in memory representation of a file
pub struct Transient {
    inner: Mutex<Inner>
}

struct Inner {
    data: Vec<u8>,
    pos: usize,
    append: bool
}

impl Transient {
    /// create a new file
    fn new (append: bool) -> Transient {
        Transient {inner: Mutex::new(Inner{data: Vec::new(), pos: 0, append})}
    }

    pub fn new_db (_name: &str, cached_data_pages: usize, bucket_fill_target: usize) -> Result<Box<dyn HammersbaldAPI>, Error> {
        let log = LogFile::new(
            Box::new(AsyncFile::new(
            Box::new(Transient::new(true)))?));
        let table = TableFile::new(
            Box::new(CachedFile::new(
            Box::new(Transient::new(false)), cached_data_pages)?))?;
        let data = DataFile::new(
            Box::new(CachedFile::new(
                Box::new(AsyncFile::new(Box::new(Transient::new(true)))?),
                cached_data_pages)?))?;
        let link = DataFile::new(
            Box::new(CachedFile::new(
                Box::new(AsyncFile::new(Box::new(Transient::new(true)))?),
                cached_data_pages)?))?;
        Ok(Box::new(Hammersbald::new(log, table, data, link, bucket_fill_target)?))
    }
}

impl PagedFile for Transient {
    fn read_page(&self, pref: PRef) -> Result<Option<Page>, Error> {
        let mut inner = self.inner.lock().unwrap();
        let len = inner.seek(SeekFrom::End(0))?;
        if pref.as_u64() < len {
            inner.seek(SeekFrom::Start(pref.as_u64()))?;
            let mut buffer = [0u8; PAGE_SIZE];
            inner.read(&mut buffer)?;
            return Ok(Some(Page::from_buf(buffer)));
        }
        Ok(None)
    }

    fn len(&self) -> Result<u64, Error> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.data.len() as u64)
    }

    fn truncate(&mut self, len: u64) -> Result<(), Error> {
        if len % PAGE_SIZE as u64 != 0 {
            return Err(Error::Corrupted(format!("truncate not to page boundary {}", len)));
        }
        let mut inner = self.inner.lock().unwrap();
        inner.data.truncate(len as usize);
        Ok(())
    }

    fn sync(&self) -> Result<(), Error> { Ok(()) }

    fn shutdown (&mut self) {
    }

    fn append_page(&mut self, page: Page) -> Result<(), Error> {
        let mut inner = self.inner.lock().unwrap();
        inner.write(&page.clone().into_buf())?;
        Ok(())
    }

    fn update_page(&mut self, page: Page) -> Result<u64, Error> {
        let mut inner = self.inner.lock().unwrap();
        inner.seek(SeekFrom::Start(page.pref().as_u64()))?;
        inner.write(&page.into_buf())?;
        Ok(inner.data.len() as u64)
    }

    fn flush(&mut self) -> Result<(), Error> {Ok(())}
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
