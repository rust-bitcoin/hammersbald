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

use page::{Page, PAGE_SIZE};
use pagedfile::PagedFile;

use error::BCDBError;
use offset::{Offset, OffsetReader};

use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

use std::cmp::min;
use std::io::{Write, Read, Cursor};

/// file storing data and data extensions
pub struct DataFile {
    im: DataFileImpl
}

impl DataFile {
    /// create new file
    pub fn new(rw: Box<PagedFile>) -> Result<DataFile, BCDBError> {
        Ok(DataFile{im: DataFileImpl::new(rw)?})
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
    pub fn iter<'a>(&'a self) -> impl Iterator<Item=(Offset, Vec<u8>, Vec<u8>)> + 'a {
        DataFileIterator::new(DataIterator::new(
            DataPageIterator::new(&self.im, 0), 2))
    }

    /// get a stored content at offset
    pub fn get_content(&self, offset: Offset) -> Result<Option<Content>, BCDBError> {
        self.im.get_content(offset)
    }

    /// append data
    pub fn append_data (&mut self, key: &[u8], data: &[u8]) -> Result<Offset, BCDBError> {
        self.im.append(DataEntry::new_data(key, data))
    }

    /// append extension
    pub fn append_data_extension (&mut self, data: &[u8]) -> Result<Offset, BCDBError> {
        self.im.append(DataEntry::new_data_extension(data))
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
    type Item = (Offset, Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        match self.inner.next() {
            Some((offset, Content::Data(key, data))) => Some((offset, key, data)),
            Some((offset, Content::Extension(data))) => Some((offset, Vec::new(), data)),
            Some(_) => None,
            None => None
        }
    }
}

/// the data file
pub(crate) struct DataFileImpl {
    file: Box<PagedFile>,
    append_pos: Offset,
    page: Page,
}

impl DataFileImpl {
    /// create a new data file
    pub fn new(file: Box<PagedFile>) -> Result<DataFileImpl, BCDBError> {
        let append_pos = Offset::from(file.len()?);
        Ok(DataFileImpl {
            file,
            append_pos,
            page: Page::new()})
    }

    /// initialize
    pub fn init(&mut self, magic: [u8; 2]) -> Result<(), BCDBError> {
        self.append_slice(&magic)?;
        Ok(())
    }

    /// shutdown
    pub fn shutdown (&mut self) {
        self.file.shutdown()
    }

    /// get a stored content at offset
    pub fn get_content(&self, offset: Offset) -> Result<Option<Content>, BCDBError> {
        let mut fetch_iterator = DataIterator::new(
            DataPageIterator::new(&self, offset.page_number()), offset.in_page_pos());
        match fetch_iterator.next () {
            None => Ok(None),
            Some(d) => Ok(Some(d.1))
        }
    }

    pub(crate) fn append (&mut self, entry: DataEntry) -> Result<Offset, BCDBError> {
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
        while wrote < slice.len() {
            let pos = self.append_pos.in_page_pos();
            let have = min(slice.len() - wrote, PAGE_SIZE - pos);
            self.page.payload[pos..pos + have].copy_from_slice(&slice[wrote..wrote + have]);
            wrote += have;
            self.append_pos = Offset::from(self.append_pos.as_u64() + have as u64);
            if self.append_pos.this_page() == self.append_pos {
                let page = self.page.clone();
                self.append_page(page)?;
                self.page.payload[0..PAGE_SIZE].copy_from_slice(&[0u8; PAGE_SIZE]);
            }
        }
        Ok(())
    }
}

impl PagedFile for DataFileImpl {
    fn flush(&mut self) -> Result<(), BCDBError> {
        if self.append_pos.in_page_pos() > 0 {
            let page = self.page.clone();
            self.append_page(page)?;
            self.append_pos = self.append_pos.this_page() + PAGE_SIZE as u64;
            self.page.payload[0..PAGE_SIZE].copy_from_slice(&[0u8; PAGE_SIZE]);
        }
        self.file.flush()
    }

    fn len(&self) -> Result<u64, BCDBError> {
        self.file.len()
    }

    fn truncate(&mut self, len: u64) -> Result<(), BCDBError> {
        self.append_pos = Offset::from(len);
        self.page.payload[0..PAGE_SIZE].copy_from_slice(&[0u8; PAGE_SIZE]);
        self.file.truncate(len)
    }

    fn sync(&self) -> Result<(), BCDBError> {
        self.file.sync()
    }

    fn read_page(&self, offset: Offset) -> Result<Option<Page>, BCDBError> {
        if offset == self.append_pos.this_page() {
            return Ok(Some(self.page.clone()))
        }
        self.file.read_page(offset)
    }

    fn append_page(&mut self, page: Page) -> Result<(), BCDBError> {
        self.file.append_page(page)
    }

    fn write_page(&mut self, _: Offset, _: Page) -> Result<u64, BCDBError> {
        unimplemented!()
    }

    fn shutdown (&mut self) {
    }
}

/// iterate through pages of a paged file
pub(crate) struct DataPageIterator<'file> {
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
    /// regular data
    Data(Vec<u8>, Vec<u8>),
    /// data referred by data, not in index
    Extension(Vec<u8>)
}

/// types of data stored in the data file
#[derive(Eq, PartialEq,Debug,Copy, Clone)]
pub(crate) enum DataType {
    /// no data, just padding the storage pages with zero bytes
    Padding,
    /// application defined data
    Data,
    /// Spillover bucket of the hash table
    Link,
    /// Application data extension without key
    Extension
}

impl DataType {
    pub fn from (data_type: u8) -> DataType {
        match data_type {
            1 => DataType::Data,
            2 => DataType::Link,
            3 => DataType::Extension,
            _ => DataType::Padding
        }
    }

    pub fn to_u8 (&self) -> u8 {
        match *self {
            DataType::Padding => 0,
            DataType::Data => 1,
            DataType::Link => 2,
            DataType::Extension => 3
        }
    }
}

#[derive(Eq, PartialEq,Debug,Clone)]
pub(crate) struct DataEntry {
    pub data_type: DataType,
    pub data: Vec<u8>
}

impl DataEntry {
    pub fn new_data (key: &[u8], data: &[u8]) -> DataEntry {
        let mut content = Vec::new();
        content.write_u8(key.len() as u8).unwrap();
        content.write(key).unwrap();
        content.extend(data);
        DataEntry{data_type: DataType::Data, data: content}
    }

    pub fn new_data_extension (data: &[u8]) -> DataEntry {
        DataEntry{data_type: DataType::Extension, data: data.to_vec()}
    }
}

pub(crate) struct DataIterator<'file> {
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
    type Item = (Offset, Content);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current.is_none() {
            self.current = self.page_iterator.next();
        }
        loop {
            let data_type;
            let offset = Offset::from(self.pos as u64 + (self.page_iterator.pagenumber-1) * PAGE_SIZE as u64);
            if let Some(t) = self.read(1) {
                data_type = DataType::from(t[0]);
            }
            else {
                return None;
            }
            if data_type == DataType::Data {
                if let Some(buf) = self.read_sized() {
                    let mut cursor = Cursor::new(buf);
                    let klen = cursor.read_u8().unwrap() as usize;
                    let mut key = vec!(0u8; klen);
                    cursor.read(&mut key).unwrap();
                    let mut data = Vec::new();
                    cursor.read_to_end(&mut data).unwrap();
                    return Some((offset, Content::Data(key, data)));
                }
            } else if data_type == DataType::Extension {
                if let Some(buf) = self.read_sized() {
                    return Some((offset, Content::Extension(buf)));
                }
            } else if data_type == DataType::Link {
                if let Some(buf) = self.read_sized() {
                    let mut cursor = Cursor::new(buf);
                    let m = cursor.read_u8().unwrap() as usize;
                    let mut hashes = Vec::new();
                    for _ in 0..m {
                        hashes.push(cursor.read_u32::<BigEndian>().unwrap());
                    }
                    let mut offsets = Vec::new();
                    for _ in 0..m {
                        offsets.push(cursor.read_offset());
                    }
                    let next = cursor.read_offset();
                    let links = hashes.iter().zip(offsets.iter())
                        .map(|(h, o)| (*h, *o)).collect();
                    return Some((offset, Content::Link(links, next)));
                }
            }
        }
    }
}

