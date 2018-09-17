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
//! # The data file
//! Specific implementation details to data file
//!

use asyncfile::AsyncFile;
use bcdb::{PageIterator, PageFile, KEY_LEN};
use page::{Page, PAYLOAD_MAX};
use error::BCSError;
use types::{Offset, U24};
use cache::Cache;

use std::cmp::min;
use std::sync::{Arc, Condvar, Mutex};
use std::cell::Cell;
use std::thread;


/// The key file
pub struct DataFile {
    async_file: DataPageFile,
    append_pos: Offset,
    page: Page
}

impl DataFile {
    pub fn new(rw: Box<PageFile>) -> Result<DataFile, BCSError> {
        let file = DataPageFile::new(rw);
        let append_pos = Offset::new(file.len()?)?;
        Ok(DataFile{async_file: file,
            append_pos,
            page: Page::new(append_pos) })
    }

    pub fn init(&mut self) -> Result<(), BCSError> {
        if self.append_pos.as_u64() == 0 {
            self.append_slice(&[0xBC,0xDA])?;
        }
        Ok(())
    }

    pub fn shutdown (&mut self) {
        self.async_file.shutdown()
    }

    fn page_iter (&self, pagenumber: u64) -> PageIterator {
        PageIterator::new(self, pagenumber)
    }

    pub fn data_iter (&self) -> DataIterator {
        DataIterator::new(self.page_iter(0), 2)
    }

    pub fn get (&self, offset: Offset) -> Result<Option<DataEntry>, BCSError> {
        let page = {
            if self.page.offset == offset.this_page() {
                self.page.clone()
            }
            else {
                self.read_page(offset.this_page())?
            }
        };
        let mut fetch_iterator = DataIterator::new_fetch(
            PageIterator::new(self, offset.page_number()+1), offset.in_page_pos(), page);
        if let Some(entry) = fetch_iterator.next() {
            if entry.data_type == DataType::AppData || entry.data_type == DataType::AppDataExtension {
                return Ok(Some(entry));
            }
            else {
                return Err(BCSError::Corrupted(format!("expected data at {}", offset.as_u64())));
            }
        }
        return Ok(None);
    }

    pub fn get_spillover (&self, offset: Offset) -> Result<(Offset, Offset), BCSError> {
        let page = {
            if self.page.offset == offset.this_page() {
                self.page.clone()
            }
            else {
                self.read_page(offset.this_page())?
            }
        };
        let mut fetch_iterator = DataIterator::new_fetch(
            PageIterator::new(self, offset.page_number()+1), offset.in_page_pos(), page);
        if let Some(entry) = fetch_iterator.next() {
            if entry.data_type != DataType::TableSpillOver {
                return Err(BCSError::Corrupted(format!("expected spillover {}", offset.as_u64())))
            }
            return Ok((Offset::from_slice(&entry.data[..6])?, Offset::from_slice(&entry.data[6..])?));
        }
        return Err(BCSError::Corrupted(format!("can not find spillover {}", offset.as_u64())))
    }

    pub fn append (&mut self, entry: DataEntry) -> Result<Offset, BCSError> {
        if entry.data_type == DataType::AppData && entry.data_key.len() != KEY_LEN {
            return Err(BCSError::DoesNotFit);
        }

        let start = self.append_pos;
        let mut data_type = [0u8;1];
        data_type[0] = entry.data_type.to_u8();
        self.append_slice(&data_type)?;


        let mut len = [0u8; 3];
        if entry.data_type == DataType::AppData {
            U24::new(KEY_LEN + entry.data.len())?.serialize(&mut len);
        }
        else {
            U24::new(entry.data.len())?.serialize(&mut len);
        }
        self.append_slice(&len)?;
        self.append_slice(entry.data_key.as_slice())?;
        self.append_slice(entry.data.as_slice())?;
        return Ok(start);
    }

    fn append_slice (&mut self, slice: &[u8]) -> Result<(), BCSError> {
        let mut wrote = 0;
        let mut wrote_on_this_page = 0;
        let mut pos = self.append_pos.in_page_pos();
        while wrote < slice.len() {
            let have = min(slice.len() - wrote, PAYLOAD_MAX - pos);
            self.page.payload[pos..pos + have].copy_from_slice(&slice[wrote..wrote + have]);
            pos += have;
            wrote += have;
            wrote_on_this_page += have;
            if pos == PAYLOAD_MAX {
                self.async_file.append_page(self.page.clone())?;
                self.append_pos = self.append_pos.next_page()?;
                self.page.offset = self.append_pos;
                pos = 0;
                wrote_on_this_page = 0;
            }
        }
        self.append_pos = Offset::new(self.append_pos.as_u64() + wrote_on_this_page as u64)?;
        Ok(())
    }

    pub fn clear_cache(&mut self) {
        self.async_file.clear_cache();
    }
}

struct DataPageFile {
    inner: Arc<DataPageFileInner>
}

struct DataPageFileInner {
    file: Mutex<Box<PageFile>>,
    cache: Mutex<Cache>,
    flushed: Condvar,
    work: Condvar,
    run: Mutex<Cell<bool>>
}

impl DataPageFileInner {
    pub fn new (file: Box<PageFile>) -> DataPageFileInner {
        DataPageFileInner { file: Mutex::new(file), cache: Mutex::new(Cache::default()), flushed: Condvar::new(), work: Condvar::new(), run: Mutex::new(Cell::new(true)) }
    }
}

impl DataPageFile {
    pub fn new (file: Box<PageFile>) -> DataPageFile {
        let inner = Arc::new(DataPageFileInner::new(file));
        let inner2 = inner.clone();
        thread::spawn(move || { DataPageFile::background(inner2) });
        DataPageFile { inner }
    }

    fn background (inner: Arc<DataPageFileInner>) {
        let mut run = true;
        while run {
            let mut cache = inner.cache.lock().unwrap();
            while run && cache.is_empty() {
                inner.flushed.notify_all();
                cache = inner.work.wait(cache).unwrap();
                run = inner.run.lock().unwrap().get();
            }
            if run {
                let writes = cache.writes().into_iter().map(|e| e.clone()).collect::<Vec<_>>();
                cache.move_writes_to_wrote();
                for (_, page) in writes {
                    use std::ops::Deref;
                    inner.file.lock().unwrap().append_page(page.deref().clone()).unwrap();
                }
            }
        }
    }

    fn read_page_from_store (&self, offset: Offset) -> Result<Page, BCSError> {
        self.inner.file.lock().unwrap().read_page(offset)
    }

    pub fn shutdown (&mut self) {
        self.inner.run.lock().unwrap().set(false);
        self.inner.work.notify_one();
    }

    pub fn clear_cache(&mut self) {
        self.inner.cache.lock().unwrap().clear();
    }
}

impl PageFile for DataPageFile {
    fn flush(&mut self) -> Result<(), BCSError> {
        let mut cache = self.inner.cache.lock().unwrap();
        cache = self.inner.flushed.wait(cache).unwrap();
        self.inner.file.lock().unwrap().flush()
    }

    fn len(&self) -> Result<u64, BCSError> {
        self.inner.file.lock().unwrap().len()
    }

    fn truncate(&mut self, new_len: u64) -> Result<(), BCSError> {
        self.inner.file.lock().unwrap().truncate(new_len)
    }

    fn sync(&self) -> Result<(), BCSError> {
        self.inner.file.lock().unwrap().sync()
    }

    fn read_page(&self, offset: Offset) -> Result<Page, BCSError> {
        {
            use std::ops::Deref;

            let cache = self.inner.cache.lock().unwrap();
            if let Some(page) = cache.get(offset) {
                return Ok(page.deref().clone());
            }
        }
        let page = self.read_page_from_store(offset)?;
        {
            // if there was a write between above read and this lock
            // then this cache is irrelevant as write cache has priority
            let mut cache = self.inner.cache.lock().unwrap();
            cache.cache(page.clone());
        }
        Ok(page)
    }

    fn append_page(&mut self, page: Page) -> Result<(), BCSError> {
        self.inner.cache.lock().unwrap().append(page);
        self.inner.work.notify_one();
        Ok(())
    }

    fn write_page(&mut self, page: Page) -> Result<(), BCSError> {
        self.inner.cache.lock().unwrap().update(page);
        self.inner.work.notify_one();
        Ok(())
    }
}

impl PageFile for DataFile {
    fn flush(&mut self) -> Result<(), BCSError> {
        if self.append_pos.in_page_pos() > 0 {
            self.async_file.append_page(self.page.clone())?;
            self.append_pos = self.append_pos.next_page()?;
            self.page.offset = self.append_pos;
        }
        self.async_file.flush()
    }

    fn len(&self) -> Result<u64, BCSError> {
        self.async_file.len()
    }

    fn truncate(&mut self, len: u64) -> Result<(), BCSError> {
        self.async_file.truncate(len)
    }

    fn sync(&self) -> Result<(), BCSError> {
        self.async_file.sync()
    }

    fn read_page(&self, offset: Offset) -> Result<Page, BCSError> {
        if offset == self.page.offset {
            return Ok(self.page.clone())
        }
        if offset.as_u64() >= self.page.offset.as_u64() {
            return Err(BCSError::Corrupted(format!("Read past EOF on data {}", offset.as_u64())));
        }
        self.async_file.read_page(offset)
    }

    fn append_page(&mut self, page: Page) -> Result<(), BCSError> {
        self.async_file.append_page(page)
    }

    fn write_page(&mut self, _: Page) -> Result<(), BCSError> {
        unimplemented!()
    }
}

/// types of data stored in the data file
#[derive(Eq, PartialEq,Debug,Copy, Clone)]
pub enum DataType {
    /// no data, just padding the storage pages with zero bytes
    Padding,
    /// application defined data
    AppData,
    /// Spillover bucket of the hash table
    TableSpillOver,
    /// Application data extension without key
    AppDataExtension
}

impl DataType {
    pub fn from (data_type: u8) -> DataType {
        match data_type {
            1 => DataType::AppData,
            2 => DataType::TableSpillOver,
            3 => DataType::AppDataExtension,
            _ => DataType::Padding
        }
    }

    pub fn to_u8 (&self) -> u8 {
        match *self {
            DataType::Padding => 0,
            DataType::AppData => 1,
            DataType::TableSpillOver => 2,
            DataType::AppDataExtension => 3
        }
    }
}

#[derive(Eq, PartialEq,Debug,Clone)]
pub struct DataEntry {
    pub data_type: DataType,
    pub data_key: Vec<u8>,
    pub data: Vec<u8>
}

impl DataEntry {
    pub fn new_data (data_key: &[u8], data: &[u8]) -> DataEntry {
        DataEntry{data_type: DataType::AppData, data_key: data_key.to_vec(), data: data.to_vec()}
    }
    pub fn new_data_extension (data: &[u8]) -> DataEntry {
        DataEntry{data_type: DataType::AppDataExtension, data_key: Vec::new(), data: data.to_vec()}
    }

    pub fn new_spillover (offset: Offset, next: Offset) -> DataEntry {
        let mut sp = [0u8; 12];
        offset.serialize(&mut sp[..6]);
        next.serialize(&mut sp[6..]);
        DataEntry{data_type: DataType::TableSpillOver, data_key: Vec::new(), data: sp.to_vec()}
    }
}

pub struct DataIterator<'file> {
    page_iterator: PageIterator<'file>,
    current: Option<Page>,
    pos: usize
}

impl<'file> DataIterator<'file> {
    pub fn new (page_iterator: PageIterator<'file>, pos: usize) -> DataIterator {
        DataIterator{page_iterator, pos, current: None}
    }

    pub fn new_fetch (page_iterator: PageIterator<'file>, pos: usize, page: Page) -> DataIterator {
        DataIterator{page_iterator, pos, current: Some(page)}
    }

    fn skip_non_data(&mut self) -> Option<DataType> {
        loop {
            if let Some(ref mut current) = self.current {
                while self.pos < PAYLOAD_MAX {
                    let data_type = DataType::from(current.payload[self.pos]);
                    self.pos += 1;
                    if data_type == DataType::AppData {
                        return Some(data_type);
                    }
                    if data_type == DataType::AppDataExtension {
                        return Some(data_type);
                    }
                    if data_type == DataType::TableSpillOver {
                        return Some(data_type);
                    }
                }
            }
            else {
                return None;
            }
            self.current = self.page_iterator.next();
            self.pos = 0;
        }
    }

    fn read_slice (&mut self, slice: &mut [u8]) -> bool {
        let mut read = 0;
        loop {
            let have = min(PAYLOAD_MAX - self.pos, slice.len() - read);
            if let Some(ref mut current) = self.current {
                slice[read .. read + have].copy_from_slice(&current.payload[self.pos .. self.pos + have]);
                self.pos += have;
                read += have;

                if read == slice.len() {
                    return true;
                }
            }
            else {
                return false;
            }
            if read < slice.len() {
                self.current = self.page_iterator.next();
                self.pos = 0;
            }
        }
    }
}

impl<'file> Iterator for DataIterator<'file> {
    type Item = DataEntry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current.is_none() {
            self.current = self.page_iterator.next();
        }
        if self.current.is_some() {
            if let Some(data_type) = self.skip_non_data() {
                if data_type == DataType::AppData {
                    let mut size = [0u8; 3];
                    if self.read_slice(&mut size) {
                        let len = U24::from_slice(&size).unwrap();
                        let mut buf = vec!(0u8; len.as_usize());
                        if self.read_slice(buf.as_mut_slice()) {
                            return Some(
                                DataEntry::new_data(&buf[0..KEY_LEN], &buf[KEY_LEN..]));
                        }
                    }
                }
                if data_type == DataType::AppDataExtension {
                    let mut size = [0u8; 3];
                    if self.read_slice(&mut size) {
                        let len = U24::from_slice(&size).unwrap();
                        let mut buf = vec!(0u8; len.as_usize());
                        if self.read_slice(buf.as_mut_slice()) {
                            return Some(
                                DataEntry::new_data_extension(&buf[..]));
                        }
                    }
                }
                else if data_type == DataType::TableSpillOver {
                    let mut size = [0u8; 3];
                    if self.read_slice(&mut size) {
                        let mut data = [0u8; 6];
                        let mut next = [0u8; 6];
                        if self.read_slice(&mut data) && self.read_slice(&mut next) {
                            return Some(
                                DataEntry::new_spillover(Offset::from_slice(&data[..]).unwrap(),
                                                         Offset::from_slice(&next[..]).unwrap()));
                        }
                    }
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod test {
    extern crate hex;

    use super::*;
    use inmemory::InMemory;

    #[test]
    fn test() {
        let mem = InMemory::new(true);
        let mut data = DataFile::new(Box::new(mem)).unwrap();
        data.init().unwrap();
        assert!(data.page_iter(0).next().is_some());
        assert!(data.data_iter().next().is_none());
        let entry = DataEntry::new_data(&[0u8;KEY_LEN], "hello world!".as_bytes());
        let hello_offset = data.append(entry.clone()).unwrap();
        let big_entry = DataEntry::new_data(&[1u8;KEY_LEN], vec!(1u8; 5000).as_slice());
        let big_offset = data.append(big_entry.clone()).unwrap();
        data.flush().unwrap();
        {
            let mut iter = data.data_iter();
            assert_eq!(iter.next().unwrap(), entry.clone());
            assert_eq!(iter.next().unwrap(), big_entry.clone());
            assert!(iter.next().is_none());
        }
        assert_eq!(data.get(hello_offset).unwrap().unwrap(), entry);
        assert_eq!(data.get(big_offset).unwrap().unwrap(), big_entry);
        data.sync().unwrap();
    }
}