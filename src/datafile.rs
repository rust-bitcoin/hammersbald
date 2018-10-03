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
use types::{Offset, OffsetReader};
use cache::Cache;

use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

use std::cmp::min;
use std::sync::{Arc, Condvar, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::io::{Write, Read, Cursor};

/// file storing data and data extensions
pub struct DataFile {
    im: DataFileImpl
}

impl DataFile {
    /// create new file
    pub fn new(rw: Box<PageFile>) -> Result<DataFile, BCDBError> {
        Ok(DataFile{im: DataFileImpl::new(rw, "data")?})
    }

    /// initialize
    pub fn init(&mut self) -> Result<(), BCDBError> {
        self.im.init ([0xBC, 0xDA])
    }

    /// shutdown
    pub fn shutdown (&mut self) {
        self.im.shutdown()
    }

    /// get an iterator of data
    pub fn iter<'a>(&'a self) -> impl Iterator<Item=(Option<Vec<Vec<u8>>>, Vec<u8>)> + 'a {
        DataFileIterator::new(DataIterator::new(
            DataPageIterator::new(&self.im, 0), 2))
    }

    /// get a stored content at offset
    pub fn get_content(&self, offset: Offset) -> Result<Option<Content>, BCDBError> {
        self.im.get_content(offset)
    }

    /// append data
    pub fn append_data (&mut self, keys: Vec<Vec<u8>>, data: &[u8]) -> Result<Offset, BCDBError> {
        self.im.append(DataEntry::new_data(keys, data))
    }

    /// append extension
    pub fn append_data_extension (&mut self, data: &[u8]) -> Result<Offset, BCDBError> {
        self.im.append(DataEntry::new_data_extension(data))
    }

    /// clear cache
    pub fn clear_cache(&mut self, len: u64) {
        self.im.clear_cache(len);
    }

    /// truncate file
    pub fn truncate(&mut self, offset: u64) -> Result<(), BCDBError> {
        self.im.truncate (offset)
    }

    /// flush buffers
    pub fn flush (&mut self) -> Result<(), BCDBError> {
        self.im.flush()
    }

    /// sync file on file system
    pub fn sync (&self) -> Result<(), BCDBError> {
        self.im.sync()
    }

    /// get file length
    pub fn len (&self) -> Result<u64, BCDBError> {
        self.im.len()
    }
}

struct DataFileIterator<'a> {
    inner: DataIterator<'a>
}

impl<'a> DataFileIterator<'a> {
    fn new (inner: DataIterator) -> DataFileIterator {
        DataFileIterator{inner}
    }
}

impl<'a> Iterator for DataFileIterator<'a> {
    type Item = (Option<Vec<Vec<u8>>>, Vec<u8>);

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        match self.inner.next() {
            Some(Content::Data(k, v)) => Some((Some(k), v)),
            Some(Content::Extension(d)) => Some((None, d)),
            Some(_) => None,
            None => None
        }
    }
}

/// file storing data link chains from hash table to data
pub struct LinkFile {
    im: DataFileImpl
}

impl LinkFile {
    /// create new file
    pub fn new(rw: Box<PageFile>) -> Result<LinkFile, BCDBError> {
        Ok(LinkFile{im: DataFileImpl::new(rw, "link")?})
    }

    /// initialize
    pub fn init(&mut self) -> Result<(), BCDBError> {
        self.im.init ([0xBC, 0xDB])
    }

    /// shutdown
    pub fn shutdown (&mut self) {
        self.im.shutdown()
    }

    /// get an iterator of links
    pub fn iter<'a>(&'a self) -> impl Iterator<Item=(Vec<Offset>, Offset)> + 'a {
        LinkFileIterator::new(DataIterator::new(
            DataPageIterator::new(&self.im, 0), 2))
    }


    /// get a stored content at offset
    pub fn get_content(&self, offset: Offset) -> Result<Option<Content>, BCDBError> {
        self.im.get_content(offset)
    }

    /// append data
    pub fn append_link (&mut self, links: Vec<(u32, Offset)>, next: Offset) -> Result<Offset, BCDBError> {
        self.im.append(DataEntry::new_link(links, next))
    }

    /// get a link
    pub fn get_link(&self, offset: Offset) -> Result<(Vec<Offset>, Offset), BCDBError> {
        match self.im.get_content(offset)? {
            Some(Content::Link(current, next)) => Ok((
                current.iter().fold(Vec::new(), |mut a, e| {a.push(e.1); a}), next)),
            Some(_) | None => Err(BCDBError::Corrupted(format!("can not find link {}", offset)))
        }
    }

    /// clear cache
    pub fn clear_cache(&mut self, len: u64) {
        self.im.clear_cache(len);
    }

    /// truncate file
    pub fn truncate(&mut self, offset: u64) -> Result<(), BCDBError> {
        self.im.truncate (offset)
    }

    /// flush buffers
    pub fn flush (&mut self) -> Result<(), BCDBError> {
        self.im.flush()
    }

    /// sync file on file system
    pub fn sync (&self) -> Result<(), BCDBError> {
        self.im.sync()
    }

    /// get file length
    pub fn len (&self) -> Result<u64, BCDBError> {
        self.im.len()
    }
}

struct LinkFileIterator<'a> {
    inner: DataIterator<'a>
}

impl<'a> LinkFileIterator<'a> {
    fn new (inner: DataIterator) -> LinkFileIterator {
        LinkFileIterator{inner}
    }
}

impl<'a> Iterator for LinkFileIterator<'a> {
    type Item = (Vec<Offset>, Offset);

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        match self.inner.next() {
            Some(Content::Link(current, next)) => {
                Some (
                    (current.iter().fold(Vec::new(), |mut a, e| {a.push(e.1); a}), next))
            },
            Some(_) => None,
            None => None
        }
    }
}

/// the data file
pub struct DataFileImpl {
    async_file: DataPageFile,
    append_pos: Offset,
    page: Page,
    #[allow(dead_code)]
    role: String,
}

impl DataFileImpl {
    /// create a new data file
    pub fn new(rw: Box<PageFile>, role: &str) -> Result<DataFileImpl, BCDBError> {
        let file = DataPageFile::new(rw)?;
        let append_pos = Offset::from(file.len()?);
        Ok(DataFileImpl {async_file: file,
            append_pos,
            page: Page::new(), role: role.to_string()})
    }

    /// initialize
    pub fn init(&mut self, magic: [u8; 2]) -> Result<(), BCDBError> {
        self.append_slice(&magic)?;
        Ok(())
    }

    /// shutdown
    pub fn shutdown (&mut self) {
        self.async_file.shutdown()
    }

    /// get an iterator of data
    pub fn iter<'a>(&'a self) -> impl Iterator<Item=Content> + 'a {
        DataIterator::new(DataPageIterator::new(&self, 0), 2)
    }

    /// get a stored content at offset
    pub fn get_content(&self, offset: Offset) -> Result<Option<Content>, BCDBError> {
        let mut fetch_iterator = DataIterator::new(
            DataPageIterator::new(&self, offset.page_number()), offset.in_page_pos());
        Ok(fetch_iterator.next())
    }

    fn append (&mut self, entry: DataEntry) -> Result<Offset, BCDBError> {
        let start= self.append_pos;
        let mut pack = Vec::new();
        pack.write_u8(entry.data_type.to_u8())?;
        pack.write_u24::<BigEndian>(entry.data.len() as u32)?;
        pack.extend(entry.data);
        self.append_slice(pack.as_slice())?;
        return Ok(start);
    }

    fn append_slice (&mut self, slice: &[u8]) -> Result<(), BCDBError> {
        let mut wrote = 0;
        let mut pos = self.append_pos.in_page_pos();
        while wrote < slice.len() {
            let have = min(slice.len() - wrote, PAGE_SIZE - pos);
            self.page.payload[pos..pos + have].copy_from_slice(&slice[wrote..wrote + have]);
            pos += have;
            wrote += have;
            if pos == PAGE_SIZE && wrote < slice.len() {
                let page = self.page.clone();
                self.append_page(page)?;
                self.page.payload[0..PAGE_SIZE].copy_from_slice(&[0u8; PAGE_SIZE]);
                pos = 0;
            }
        }
        if pos == PAGE_SIZE {
            let page = self.page.clone();
            self.append_page(page)?;
            self.page.payload[0..PAGE_SIZE].copy_from_slice(&[0u8; PAGE_SIZE]);
        }
        self.append_pos = Offset::from(self.append_pos.as_u64() + slice.len() as u64);
        Ok(())
    }

    fn clear_cache(&mut self, len: u64) {
        self.async_file.clear_cache(len);
    }
}

impl PageFile for DataFileImpl {
    fn flush(&mut self) -> Result<(), BCDBError> {
        if self.append_pos.in_page_pos() > 0 {
            let page = self.page.clone();
            self.append_page(page)?;
            self.page.payload[0..PAGE_SIZE].copy_from_slice(&[0u8; PAGE_SIZE]);
            if self.append_pos != self.append_pos.this_page() {
                self.append_pos = Offset::from(self.append_pos.this_page().as_u64() + PAGE_SIZE as u64);
            }
        }
        self.async_file.flush()
    }

    fn len(&self) -> Result<u64, BCDBError> {
        self.async_file.len()
    }

    fn truncate(&mut self, len: u64) -> Result<(), BCDBError> {
        self.append_pos = Offset::from(len);
        self.page.payload[0..PAGE_SIZE].copy_from_slice(&[0u8; PAGE_SIZE]);
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


struct DataPageFile {
    inner: Arc<DataPageFileInner>
}

struct DataPageFileInner {
    file: Mutex<Box<PageFile>>,
    cache: Mutex<Cache>,
    flushed: Condvar,
    work: Condvar,
    run: AtomicBool,
    flushing: AtomicBool
}

impl DataPageFileInner {
    pub fn new (file: Box<PageFile>) -> Result<DataPageFileInner, BCDBError> {
        let len = file.len()?;
        Ok(DataPageFileInner { file: Mutex::new(file), cache: Mutex::new(Cache::new(len)),
            flushed: Condvar::new(), work: Condvar::new(), run: AtomicBool::new(true),
            flushing: AtomicBool::new(false) })
    }
}

impl DataPageFile {
    pub fn new (file: Box<PageFile>) -> Result<DataPageFile, BCDBError> {
        let inner = Arc::new(DataPageFileInner::new(file)?);
        let inner2 = inner.clone();
        thread::spawn(move || { DataPageFile::background(inner2) });
        Ok(DataPageFile { inner })
    }

    fn background (inner: Arc<DataPageFileInner>) {
        while inner.run.load(Ordering::Relaxed) {
            let mut writes;
            {
                let mut cache = inner.cache.lock().expect("cache lock poisoned");
                if cache.is_empty() {
                    inner.flushed.notify_all();
                }
                if inner.flushing.swap(false, Ordering::AcqRel) == false {
                    cache = inner.work.wait(cache).expect("cache lock poisoned while waiting for work");
                }
                writes = cache.move_writes_to_wrote();
            }
            if !writes.is_empty() {
                writes.sort_unstable_by(|a, b| u64::cmp(&a.0.as_u64(), &b.0.as_u64()));
                let mut file = inner.file.lock().expect("file lock poisoned");
                let mut next = file.len().unwrap();
                for (o, page) in &writes {
                    if o.as_u64() != next as u64 {
                        panic!("non conscutive append {} {}", next, o);
                    }
                    next = o.as_u64() + PAGE_SIZE as u64;
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

    pub fn clear_cache(&mut self, len: u64) {
        self.inner.cache.lock().unwrap().clear(len);
    }
}

impl PageFile for DataPageFile {
    #[allow(unused_assignments)]
    fn flush(&mut self) -> Result<(), BCDBError> {
        let mut cache = self.inner.cache.lock().unwrap();
        if !cache.is_empty() {
            self.inner.work.notify_one();
            self.inner.flushing.store(true, Ordering::Release);
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
        self.inner.cache.lock().unwrap().append(page);
        self.inner.work.notify_one();
        Ok(())
    }

    fn write_page(&mut self, _: Offset, _: Page) -> Result<(), BCDBError> {
        unimplemented!()
    }
}

/// iterate through pages of a paged file
struct DataPageIterator<'file> {
    /// the current page of the iterator
    pub pagenumber: u64,
    file: &'file DataFileImpl
}

/// page iterator
impl<'file> DataPageIterator<'file> {
    /// create a new iterator starting at given page
    pub fn new (file: &'file DataFileImpl, pagenumber: u64) -> DataPageIterator {
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
    /// link
    Link(Vec<(u32, Offset)>, Offset),
    /// regular data referred in index
    Data(Vec<Vec<u8>>, Vec<u8>),
    /// data referred by data, not in index
    Extension(Vec<u8>)
}

/// types of data stored in the data file
#[derive(Eq, PartialEq,Debug,Copy, Clone)]
enum DataType {
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

    pub fn new_link (links: Vec<(u32, Offset)>, next: Offset) -> DataEntry {
        let mut sp = Vec::new();
        sp.write_u8(links.len() as u8).unwrap();
        sp.extend( links.iter().fold(Vec::new(), |mut buf, s| {
            buf.write_u32::<BigEndian>(s.0).unwrap();
            buf
        }));
        sp.extend( links.iter().fold(Vec::new(), |mut buf, s| {
            buf.write_u48::<BigEndian>(s.1.as_u64()).unwrap();
            buf
        }));
        sp.write_u48::<BigEndian>(next.as_u64()).unwrap();
        DataEntry{data_type: DataType::TableSpillOver, data: sp.to_vec()}
    }
}

struct DataIterator<'file> {
    page_iterator: DataPageIterator<'file>,
    current: Option<Page>,
    pos: usize
}

impl<'file> DataIterator<'file> {
    pub fn new(page_iterator: DataPageIterator<'file>, pos: usize) -> DataIterator {
        DataIterator{page_iterator, pos, current: None}
    }

    fn read_sized(&mut self) -> Option<Vec<u8>> {
        if let Some(size) = self.read(3) {
            let mut c = Cursor::new(size);
            let len = c.read_u24::<BigEndian>().unwrap();
            if let Some(buf) = self.read(len as usize) {
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
        loop {
            let data_type;
            if let Some(t) = self.read(1) {
                data_type = DataType::from(t[0]);
            }
            else {
                return None;
            }
            if data_type == DataType::AppData {
                if let Some(buf) = self.read_sized() {
                    let mut cursor = Cursor::new(buf);
                    let n_keys = cursor.read_u8().unwrap();
                    let mut keys = Vec::new();
                    for _ in 0..n_keys {
                        let key_len = cursor.read_u8().unwrap() as usize;
                        let mut key = vec!(0u8; key_len);
                        cursor.read(&mut key).unwrap();
                        keys.push(key);
                    }
                    let pos = cursor.position() as usize;
                    let v = cursor.into_inner();
                    let (_, data) = v.split_at(pos);
                    return Some(Content::Data(keys, data.to_vec()));
                }
            } else if data_type == DataType::AppDataExtension {
                if let Some(buf) = self.read_sized() {
                    return Some(Content::Extension(buf));
                }
            } else if data_type == DataType::TableSpillOver {
                if let Some(buf) = self.read_sized() {
                    let mut cursor = Cursor::new(buf);
                    let m = cursor.read_u8().unwrap() as usize;
                    let mut hashes = Vec::new();
                    for _ in 0..m {
                        hashes.push(cursor.read_u32::<BigEndian>().unwrap());
                    }
                    let mut offsets = Vec::new();
                    for _ in 0..m {
                        offsets.push(Offset::from(cursor.read_u48::<BigEndian>().unwrap()));
                    }
                    let next = cursor.read_offset();
                    let mut oi = offsets.iter();
                    let mut links = Vec::new();
                    for h in hashes {
                        let o = *oi.next().unwrap();
                        links.push((h, o));
                    }
                    return Some(Content::Link(links, next));
                }
            }
        }
    }
}
