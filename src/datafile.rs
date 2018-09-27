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

use bcdb::{ KEY_LEN};
use page::{Page, PageIterator, PageFile, PAYLOAD_MAX};
use error::BCDBError;
use types::{Offset, U24};
use cache::Cache;

use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

use std::cmp::min;
use std::sync::{Arc, Condvar, Mutex};
use std::cell::Cell;
use std::thread;
use std::time::{Duration, Instant};
use std::cmp::Ordering;
use std::io::Cursor;


/// The key file
pub struct DataFile {
    async_file: DataPageFile,
    append_pos: Offset,
    page: Page
}

impl DataFile {
    pub fn new(rw: Box<PageFile>) -> Result<DataFile, BCDBError> {
        let file = DataPageFile::new(rw);
        let append_pos = Offset::new(file.len()?)?;
        Ok(DataFile{async_file: file,
            append_pos,
            page: Page::new(append_pos) })
    }

    pub fn init(&mut self) -> Result<(), BCDBError> {
        if self.append_pos.as_u64() == 0 {
            self.append_slice(&[0xBC,0xDA])?;
        }
        Ok(())
    }

    pub fn shutdown (&mut self) {
        self.async_file.shutdown()
    }

    pub fn get_content(&self, offset: Offset) -> Result<Content, BCDBError> {
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
            if entry.data_type == DataType::AppData {
                let (k, d) = entry.data.split_at(KEY_LEN);
                return Ok(Content::Data(k.to_vec(), d.to_vec()));
            }
            else if entry.data_type == DataType::TableSpillOver {
                let mut cursor = Cursor::new(entry.data);
                let hash = cursor.read_u32::<BigEndian>().unwrap();
                return Ok(Content::Spillover(hash, Offset::from_slice(&cursor.get_ref()[4..10])?, Offset::from_slice(&cursor.get_ref()[10..16])?))
            }
            return Ok(Content::Extension(entry.data))
        }
        return Err(BCDBError::Corrupted(format!("expected content at {}", offset.as_u64())));
    }

    pub fn append_content(&mut self, content: Content) -> Result<Offset, BCDBError> {
        match content {
            Content::Data(k, d) => self.append(DataEntry::new_data(k.as_slice(), d.as_slice())),
            Content::Extension(d) => self.append(DataEntry::new_data_extension(d.as_slice())),
            Content::Spillover(hash, data, next) => self.append(DataEntry::new_spillover(hash, data, next))
        }
    }

    fn append (&mut self, entry: DataEntry) -> Result<Offset, BCDBError> {
        let start = self.append_pos;
        let mut data_type = [0u8;1];
        data_type[0] = entry.data_type.to_u8();
        self.append_slice(&data_type)?;


        let mut len = [0u8; 3];
        U24::new(entry.data.len())?.serialize(&mut len);
        self.append_slice(&len)?;
        self.append_slice(entry.data.as_slice())?;
        return Ok(start);
    }

    fn append_slice (&mut self, slice: &[u8]) -> Result<(), BCDBError> {
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
        let mut last_loop = Instant::now();
        while run {
            run = inner.run.lock().expect("run lock poisoned").get();
            let mut writes;
            loop {
                let mut cache = inner.cache.lock().expect("cache lock poisoned");
                if cache.is_empty() {
                    inner.flushed.notify_all();
                }
                else {
                    writes = cache.move_writes_to_wrote();
                    break;
                }
                let time_spent = Instant::now() - last_loop;
                if time_spent.cmp(&Duration::from_millis(2000)) == Ordering::Greater {
                    writes = cache.move_writes_to_wrote();
                    break;
                }
                else {
                    let (c, t) = inner.work.wait_timeout(cache, Duration::from_millis(2000) - time_spent).expect("cache lock poisoned while waiting for work");
                    if t.timed_out() {
                        cache = c;
                        writes = cache.move_writes_to_wrote();
                        break;
                    }
                }
            }
            last_loop = Instant::now();
            if !writes.is_empty() {
                writes.sort_unstable_by(|a, b| u64::cmp(&a.offset.as_u64(), &b.offset.as_u64()));
                let mut file = inner.file.lock().expect("file lock poisoned");
                for page in &writes {
                    use std::ops::Deref;
                    file.append_page(page.deref().clone()).expect("can not extend data file");
                }
            }
        }
    }

    fn read_page_from_store (&self, offset: Offset) -> Result<Page, BCDBError> {
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
    #[allow(unused_assignments)]
    fn flush(&mut self) -> Result<(), BCDBError> {
        let mut cache = self.inner.cache.lock().unwrap();
        if !cache.is_empty() {
            self.inner.work.notify_one();
            cache = self.inner.flushed.wait(cache)?;
        }
        self.inner.file.lock().unwrap().flush()
    }

    fn len(&self) -> Result<u64, BCDBError> {
        self.inner.file.lock().unwrap().len()
    }

    fn truncate(&mut self, new_len: u64) -> Result<(), BCDBError> {
        self.inner.file.lock().unwrap().truncate(new_len)
    }

    fn sync(&self) -> Result<(), BCDBError> {
        self.inner.file.lock().unwrap().sync()
    }

    fn read_page(&self, offset: Offset) -> Result<Page, BCDBError> {

        use std::ops::Deref;

        {
            let cache = self.inner.cache.lock().unwrap();
            if let Some(page) = cache.get(offset) {
                return Ok(page.deref().clone());
            }
        }

        // read outside of cache lock
        let page = self.read_page_from_store(offset)?;

        {
            // write cache takes precedence, therefore insert of outdated read will be ignored
            let mut cache = self.inner.cache.lock().unwrap();
            cache.cache(page.clone());
        }
        Ok(page)
    }

    fn append_page(&mut self, page: Page) -> Result<(), BCDBError> {
        self.inner.cache.lock().unwrap().write(page);
        self.inner.work.notify_one();
        Ok(())
    }

    fn write_page(&mut self, _: Page) -> Result<(), BCDBError> {
        unimplemented!()
    }

    fn write_batch(&mut self, _: Vec<Arc<Page>>) -> Result<(), BCDBError> {
        unimplemented!()
    }
}

impl PageFile for DataFile {
    fn flush(&mut self) -> Result<(), BCDBError> {
        if self.append_pos.in_page_pos() > 0 {
            self.async_file.append_page(self.page.clone())?;
            self.append_pos = self.append_pos.next_page()?;
            self.page.offset = self.append_pos;
        }
        self.async_file.flush()
    }

    fn len(&self) -> Result<u64, BCDBError> {
        self.async_file.len()
    }

    fn truncate(&mut self, len: u64) -> Result<(), BCDBError> {
        self.append_pos = Offset::new(len)?;
        self.page.offset = self.append_pos;
        self.async_file.truncate(len)
    }

    fn sync(&self) -> Result<(), BCDBError> {
        self.async_file.sync()
    }

    fn read_page(&self, offset: Offset) -> Result<Page, BCDBError> {
        if offset == self.page.offset {
            return Ok(self.page.clone())
        }
        if offset.as_u64() >= self.page.offset.as_u64() {
            return Err(BCDBError::Corrupted(format!("Read past EOF on data {}", offset.as_u64())));
        }
        self.async_file.read_page(offset)
    }

    fn append_page(&mut self, page: Page) -> Result<(), BCDBError> {
        self.async_file.append_page(page)
    }

    fn write_page(&mut self, _: Page) -> Result<(), BCDBError> {
        unimplemented!()
    }

    fn write_batch(&mut self, _: Vec<Arc<Page>>) -> Result<(), BCDBError> {
        unimplemented!()
    }
}

/// content of the db
pub enum Content {
    /// spillover
    Spillover(u32, Offset, Offset),
    /// regular data referred in index
    Data(Vec<u8>, Vec<u8>),
    /// data referred by data, not in index
    Extension(Vec<u8>)
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
struct DataEntry {
    pub data_type: DataType,
    pub data: Vec<u8>
}

impl DataEntry {
    pub fn new_data (data_key: &[u8], data: &[u8]) -> DataEntry {
        let mut d = data.to_vec();
        d.extend(data_key.to_vec().iter());
        DataEntry{data_type: DataType::AppData, data: d}
    }
    pub fn new_data_extension (data: &[u8]) -> DataEntry {
        DataEntry{data_type: DataType::AppDataExtension, data: data.to_vec()}
    }

    pub fn new_spillover (hash: u32, offset: Offset, next: Offset) -> DataEntry {
        let mut sp = Vec::new();
        sp.write_u32::<BigEndian>(hash).unwrap();
        let mut o = [0u8;6];
        offset.serialize(&mut o);
        sp.extend(o.iter());
        let mut n = [0u8;6];
        next.serialize(&mut n);
        sp.extend(n.iter());
        DataEntry{data_type: DataType::TableSpillOver, data: sp.to_vec()}
    }
}

struct DataIterator<'file> {
    page_iterator: PageIterator<'file>,
    current: Option<Page>,
    pos: usize
}

impl<'file> DataIterator<'file> {
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
                        let mut hash = [0u8;4];
                        let mut data = [0u8; 6];
                        let mut next = [0u8; 6];
                        if self.read_slice(&mut hash) && self.read_slice(&mut data) && self.read_slice(&mut next) {
                            let mut c = Cursor::new(hash);
                            let h = c.read_u32::<BigEndian>().unwrap();
                            return Some(
                                DataEntry::new_spillover(h, Offset::from_slice(&data[..]).unwrap(),
                                                         Offset::from_slice(&next[..]).unwrap()));
                        }
                    }
                }
            }
        }
        None
    }
}
