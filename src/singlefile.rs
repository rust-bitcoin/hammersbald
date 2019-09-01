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
//! # a single file
//!
//!

use error::Error;
use pagedfile::PagedFile;
use page::{PAGE_SIZE, Page};
use pref::PRef;

use std::sync::Mutex;
use std::fs::File;
use std::io::{Read,Write,Seek,SeekFrom};
use std::cmp::max;

pub struct SingleFile {
    file: Mutex<File>,
    base: u64,
    len: u64,
    chunk_size: u64
}

impl SingleFile {
    #[allow(unused)]
    pub fn new (mut file: File) -> Result<SingleFile, Error> {
        let len = file.seek(SeekFrom::End(0))?;
        Ok(SingleFile{file: Mutex::new(file), base: 0, len, chunk_size: 1 << 47})
    }

    pub fn new_chunk (mut file: File, base: u64, chunk_size: u64) -> Result<SingleFile, Error> {
        let len = file.seek(SeekFrom::End(0))?;
        Ok(SingleFile{file: Mutex::new(file), base, len, chunk_size})
    }
}

impl PagedFile for SingleFile {
    fn read_page(&self, pref: PRef) -> Result<Option<Page>, Error> {
        let o = pref.as_u64();
        if o < self.base || o >= self.base + self.chunk_size {
            return Err(Error::Corrupted("read from wrong file".to_string()));
        }
        let pos = o - self.base;
        if pos < self.len {
            let mut file = self.file.lock().unwrap();
            file.seek(SeekFrom::Start(pos))?;
            let mut buffer = [0u8; PAGE_SIZE];
            file.read_exact(&mut buffer[..])?;
            return Ok(Some(Page::from_buf(buffer)));
        }
        Ok(None)
    }

    fn len(&self) -> Result<u64, Error> {
        Ok(self.len)
    }

    fn truncate(&mut self, new_len: u64) -> Result<(), Error> {
        self.len = new_len;
        Ok(self.file.lock().unwrap().set_len(new_len)?)
    }

    fn sync(&self) -> Result<(), Error> {
        Ok(self.file.lock().unwrap().sync_data()?)
    }

    fn shutdown (&mut self) {}

    fn append_page(&mut self, page: Page) -> Result<(), Error> {
        let mut file = self.file.lock().unwrap();
        file.write_all(&page.into_buf()[..])?;
        self.len += PAGE_SIZE as u64;
        Ok(())
    }

    fn update_page(&mut self, page: Page) -> Result<u64, Error> {
        let o = page.pref().as_u64();
        if o < self.base || o >= self.base + self.chunk_size {
            return Err(Error::Corrupted("write to wrong file".to_string()));
        }
        let pos = o - self.base;

        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(pos))?;
        file.write_all(&page.into_buf())?;
        self.len = max(self.len, pos + PAGE_SIZE as u64);
        Ok(self.len)
    }

    fn flush(&mut self) -> Result<(), Error> {
        Ok(self.file.lock().unwrap().flush()?)
    }
}