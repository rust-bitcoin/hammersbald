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

use page::{Page, PageFile, PAGE_SIZE};
use error::BCDBError;
use types::{Offset, U24, OffsetReader};
use cache::Cache;

use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

use std::cmp::min;
use std::sync::{Arc, Condvar, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::io::{Write, Read, Cursor};


/// The key file
pub struct DataFile {
    async_file: DataPageFile,
    append_pos: Offset,
    page: Page,
    role: String,
}

impl DataFile {
    pub fn new(rw: Box<PageFile>, role: &str) -> Result<DataFile, BCDBError> {
        let file = DataPageFile::new(rw);
        let append_pos = Offset::from(file.len()?);
        Ok(DataFile{async_file: file,
            append_pos,
            page: Page::new(), role: role.to_string()})
    }

    pub fn init(&mut self) -> Result<(), BCDBError> {
        self.append_slice(&[0xBC, 0xDA])?;
        Ok(())
    }

    pub fn shutdown (&mut self) {
        self.async_file.shutdown()
    }

    pub fn get_content(&self, offset: Offset) -> Result<Option<Content>, BCDBError> {
        let mut fetch_iterator = DataIterator::new_fetch(
            DataPageIterator::new(&self, offset.page_number()), offset.in_page_pos());
        Ok(fetch_iterator.next())
    }

    pub fn append_content(&mut self, content: Content) -> Result<Offset, BCDBError> {
        match content {
            Content::Data(k, d) => self.append(DataEntry::new_data(k, d.as_slice())),
            Content::Extension(d) => self.append(DataEntry::new_data_extension(d.as_slice())),
            Content::Spillover(spills, next) => self.append(DataEntry::new_spillover(spills, next))
        }
    }

    fn append (&mut self, entry: DataEntry) -> Result<Offset, BCDBError> {
        let start = self.append_pos;
        let mut data_type = [0u8;1];
        data_type[0] = entry.data_type.to_u8();
        self.append_slice(&data_type)?;

        let mut len = [0u8; 3];
        U24::from(entry.data.len()).serialize(&mut len);
        self.append_slice(&len)?;
        self.append_slice(entry.data.as_slice())?;
        return Ok(start);
    }

    fn append_slice (&mut self, slice: &[u8]) -> Result<(), BCDBError> {
        let mut wrote = 0;
        let mut wrote_on_this_page = 0;
        let mut pos = self.append_pos.in_page_pos();
        while wrote < slice.len() {
            let have = min(slice.len() - wrote, PAGE_SIZE - pos);
            self.page.payload[pos..pos + have].copy_from_slice(&slice[wrote..wrote + have]);
            pos += have;
            wrote += have;
            wrote_on_this_page += have;
            if pos == PAGE_SIZE {
                self.async_file.append_page(self.page.clone())?;
                self.page.payload[0..PAGE_SIZE].copy_from_slice(&[0u8; PAGE_SIZE]);
                self.append_pos = self.append_pos.next_page();
                pos = 0;
                wrote_on_this_page = 0;
            }
        }
        self.append_pos = Offset::from(self.append_pos.as_u64() + wrote_on_this_page as u64);
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
    run: AtomicBool
}

impl DataPageFileInner {
    pub fn new (file: Box<PageFile>) -> DataPageFileInner {
        DataPageFileInner { file: Mutex::new(file), cache: Mutex::new(Cache::default()),
            flushed: Condvar::new(), work: Condvar::new(), run: AtomicBool::new(true) }
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
        while inner.run.load(Ordering::Relaxed) {
            let mut writes;
            {
                let mut cache = inner.cache.lock().expect("cache lock poisoned");
                if cache.is_empty() {
                    inner.flushed.notify_all();
                }
                cache = inner.work.wait(cache).expect("cache lock poisoned while waiting for work");
                writes = cache.move_writes_to_wrote();
            }
            if !writes.is_empty() {
                writes.sort_unstable_by(|a, b| u64::cmp(&a.0.as_u64(), &b.0.as_u64()));
                let mut file = inner.file.lock().expect("file lock poisoned");
                for (_, page) in &writes {
                    file.append_page(page.clone()).expect("can not extend data file");
                }
            }
        }
    }

    fn read_page_from_store (&self, offset: Offset) -> Result<Option<Page>, BCDBError> {
        if offset != offset.this_page () {
            return Err(BCDBError::Corrupted(format!("data or link read is not page aligned {}", offset)))
        }
        self.inner.file.lock().unwrap().read_page(offset)
    }

    pub fn shutdown (&mut self) {
        self.inner.run.store(false, Ordering::Relaxed);
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

    fn read_page(&self, offset: Offset) -> Result<Option<Page>, BCDBError> {
        {
            let cache = self.inner.cache.lock().unwrap();
            if let Some(page) = cache.get(offset) {
                return Ok(Some(page));
            }
        }
        if let Some(page) = self.read_page_from_store(offset)? {
            // write cache takes precedence therefore no problem if there was
            // a write between above read and this lock
            let mut cache = self.inner.cache.lock().unwrap();
            cache.cache(offset, page.clone());
            return Ok(Some(page));
        }
        Ok(None)
    }

    fn append_page(&mut self, page: Page) -> Result<(), BCDBError> {
        self.inner.cache.lock().unwrap().write(Offset::from(self.len()?), page);
        self.inner.work.notify_one();
        Ok(())
    }

    fn write_page(&mut self, _: Offset, _: Page) -> Result<(), BCDBError> {
        unimplemented!()
    }
}

impl PageFile for DataFile {
    fn flush(&mut self) -> Result<(), BCDBError> {
        if self.append_pos.in_page_pos() > 0 {
            let page = self.page.clone();
            self.append_page(page)?;
            self.page.payload[0..PAGE_SIZE].copy_from_slice(&[0u8; PAGE_SIZE]);
            self.append_pos = self.append_pos.next_page();
        }
        self.async_file.flush()
    }

    fn len(&self) -> Result<u64, BCDBError> {
        self.async_file.len()
    }

    fn truncate(&mut self, len: u64) -> Result<(), BCDBError> {
        self.append_pos = Offset::from(len);
        self.async_file.truncate(len)
    }

    fn sync(&self) -> Result<(), BCDBError> {
        self.async_file.sync()
    }

    fn read_page(&self, offset: Offset) -> Result<Option<Page>, BCDBError> {
        if offset == self.append_pos.this_page() {
            return Ok(Some(self.page.clone()))
        }
        self.async_file.read_page(offset)
    }

    fn append_page(&mut self, page: Page) -> Result<(), BCDBError> {
        self.async_file.append_page(page)
    }

    fn write_page(&mut self, _: Offset, _: Page) -> Result<(), BCDBError> {
        unimplemented!()
    }
}

/// iterate through pages of a paged file
struct DataPageIterator<'file> {
    /// the current page of the iterator
    pub pagenumber: u64,
    file: &'file DataFile
}

/// page iterator
impl<'file> DataPageIterator<'file> {
    /// create a new iterator starting at given page
    pub fn new (file: &'file DataFile, pagenumber: u64) -> DataPageIterator {
        DataPageIterator{pagenumber, file}
    }
}

impl<'file> Iterator for DataPageIterator<'file> {
    type Item = Page;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pagenumber < (1 << 47) / PAGE_SIZE as u64 {
            let offset = Offset::from((self.pagenumber)* PAGE_SIZE as u64);
            if let Ok(Some(page)) = self.file.read_page(offset) {
                self.pagenumber += 1;
                return Some(page);
            }
        }
        None
    }
}

/// content of the db
pub enum Content {
    /// spillover
    Spillover(Vec<(u32, Vec<Offset>)>, Offset),
    /// regular data referred in index
    Data(Vec<Vec<u8>>, Vec<u8>),
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
    pub fn new_data (keys: Vec<Vec<u8>>, data: &[u8]) -> DataEntry {
        let mut d = Vec::new();
        d.push(keys.len() as u8);
        for key in keys {
            d.push(key.len() as u8);
            d.extend(key.to_vec());
        }
        d.extend(data.to_vec());
        DataEntry{data_type: DataType::AppData, data: d}
    }
    pub fn new_data_extension (data: &[u8]) -> DataEntry {
        DataEntry{data_type: DataType::AppDataExtension, data: data.to_vec()}
    }

    pub fn new_spillover (spills: Vec<(u32, Vec<Offset>)>, next: Offset) -> DataEntry {
        let mut sp = Vec::new();
        sp.write_u8(spills.len() as u8).unwrap();
        for s in &spills {
            let hash = s.0;
            let spill = s.1.clone();
            sp.write_u32::<BigEndian>(hash).unwrap();
            sp.write_u8(spill.len() as u8).unwrap();
            for offset in spill {
                sp.extend(offset.to_vec());
            }
        }
        sp.extend(next.to_vec());
        DataEntry{data_type: DataType::TableSpillOver, data: sp.to_vec()}
    }
}

struct DataIterator<'file> {
    page_iterator: DataPageIterator<'file>,
    current: Option<Page>,
    pos: usize
}

impl<'file> DataIterator<'file> {
    pub fn new_fetch (page_iterator: DataPageIterator<'file>, pos: usize) -> DataIterator {
        DataIterator{page_iterator, pos, current: None}
    }

    fn read_sized(&mut self) -> Option<Vec<u8>> {
        if let Some(size) = self.read(3) {
            let len = U24::from(&size[..]);
            if let Some(buf) = self.read(len.as_usize()) {
                return Some(buf);
            }
        }
        None
    }


    fn read(&mut self, n: usize) -> Option<Vec<u8>> {
        let mut v = Vec::with_capacity(n);
        let mut read = 0;
        loop {
            let have = min(PAGE_SIZE - self.pos, n - read);
            if let Some(ref mut current) = self.current {
                v.write(&current.payload[self.pos .. self.pos + have]).unwrap();
                self.pos += have;
                read += have;

                if read == n {
                    return Some(v);
                }
            }
            else {
                return None;
            }
            if read < n {
                self.current = self.page_iterator.next();
                self.pos = 0;
            }
        }
    }
}

impl<'file> Iterator for DataIterator<'file> {
    type Item = Content;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current.is_none() {
            self.current = self.page_iterator.next();
        }
        if let Some(current) = self.current.clone() {
            let data_type = DataType::from(current.payload[self.pos]);
            self.pos += 1;
            if data_type == DataType::AppData {
                if let Some(buf) = self.read_sized() {
                    let mut cursor = Cursor::new(buf);
                    let n_keys = cursor.read_u8().unwrap();
                    let mut keys = Vec::new();
                    for _ in 0 .. n_keys {
                        let key_len = cursor.read_u8().unwrap() as usize;
                        let mut key = vec!(0u8;key_len);
                        cursor.read(&mut key).unwrap();
                        keys.push(key);
                    }
                    let pos = cursor.position() as usize;
                    let v = cursor.into_inner();
                    let (_, data) = v.split_at(pos);
                    return Some(Content::Data(keys, data.to_vec()));
                }
            }
            else if data_type == DataType::AppDataExtension {
                if let Some(buf) = self.read_sized() {
                    return Some(Content::Extension(buf));
                }
            }
            else if data_type == DataType::TableSpillOver {
                if let Some(buf) = self.read_sized () {
                    let mut cursor = Cursor::new(buf);
                    let m = cursor.read_u8().unwrap() as usize;
                    let mut spills = Vec::new();
                    for _ in 0 .. m {
                        let hash = cursor.read_u32::<BigEndian>().unwrap();
                        let n = cursor.read_u8().unwrap() as usize;
                        let mut offsets = Vec::new();
                        for _ in 0..n {
                            offsets.push(cursor.read_offset())
                        }
                        spills.push((hash, offsets));
                    }
                    let next = cursor.read_offset();
                    return Some(Content::Spillover(spills, next));
                }
            }
        }
        None
    }
}
