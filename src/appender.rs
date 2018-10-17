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
//! # helper to write append only files
//!

use page::{Page, PAGE_SIZE};
use pagedfile::PagedFile;
use offset::Offset;
use error::BCDBError;

use std::cmp::min;

/// Appender for PagedFile
pub struct Appender {
    file: Box<PagedFile>,
    page: Option<Page>,
    page_offset: Offset,
    append_pos: Offset,
}

impl Appender {
    /// create a new appender for a file
    pub fn new (file: Box<PagedFile>, start: Offset) -> Result<Appender, BCDBError> {
        Ok(Appender {file, page: None, page_offset: start.this_page(), append_pos: start })
    }

    /// append a slice at current position
    pub fn append_slice(&mut self, payload: &[u8]) -> Result<(), BCDBError> {
        let mut wrote = 0;
        while wrote < payload.len() {
            let pos = self.append_pos.in_page_pos();
            if self.page.is_none() {
                self.page = Some(self.file.read_page(self.page_offset)?.unwrap_or(Page::new()));
            }
            if let Some(ref mut page) = self.page {
                let space = min(PAGE_SIZE - pos, payload.len() - wrote);
                page.payload[pos .. pos + space].copy_from_slice(&payload[wrote .. wrote + space]);
                wrote += space;
                self.append_pos += space as u64;
                if self.append_pos.in_page_pos() == 0 {
                    self.file.write_page(self.page_offset, page.clone())?;
                    self.page_offset = self.append_pos;
                }
            }
            if self.append_pos.in_page_pos() == 0 {
                self.page = None;
            }
        }
        self.append_pos += payload.len() as u64;
        Ok(())
    }

    /// return next append position
    pub fn position (&self) -> Offset {
        self.append_pos
    }

    /// extend with contents from an iterator
    pub fn extend(&mut self, mut from: impl Iterator<Item=Vec<u8>>) -> Result<Offset, BCDBError> {
        while let Some(payload) = from.next() {
            self.append_slice(payload.as_slice())?;
        }
        Ok(self.append_pos)
    }
}

impl PagedFile for Appender {
    fn flush(&mut self) -> Result<(), BCDBError> {
        if self.append_pos.in_page_pos() > 0 {
            if let Some(page) = self.page.clone() {
                self.append_page(page)?;
                self.append_pos = self.append_pos.this_page() + PAGE_SIZE as u64;
                self.page_offset = self.append_pos;
            }
            self.page = None;
        }
        self.file.flush()
    }

    fn len(&self) -> Result<u64, BCDBError> {
        self.file.len()
    }

    fn truncate(&mut self, new_len: u64) -> Result<(), BCDBError> {
        self.file.truncate(new_len)
    }

    fn sync(&self) -> Result<(), BCDBError> {
        self.file.sync()
    }

    fn read_page(&self, offset: Offset) -> Result<Option<Page>, BCDBError> {
        self.file.read_page(offset)
    }

    fn append_page(&mut self, page: Page) -> Result<(), BCDBError> {
        self.file.append_page(page)
    }

    fn write_page(&mut self, offset: Offset, page: Page) -> Result<u64, BCDBError> {
        self.file.write_page(offset, page)
    }

    fn shutdown(&mut self) {
        self.file.shutdown()
    }
}
