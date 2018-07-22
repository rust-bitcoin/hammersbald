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
use bcdb::{RW, DBFile, PageIterator, PageFile, KEY_LEN};
use page::{Page, PAYLOAD_MAX};
use error::BCSError;
use types::{Offset, U24};

use std::sync::Arc;
use std::cmp::min;

/// The key file
pub struct DataFile {
    async_file: AsyncFile,
    append_pos: Offset,
    page: Page
}

impl DataFile {
    pub fn new(rw: Box<RW>) -> DataFile {
        let offset = Offset::new(0).unwrap();
        DataFile{async_file: AsyncFile::new(rw, None),
            append_pos: offset,
            page: Page::new(offset) }
    }

    pub fn init(&mut self) -> Result<(), BCSError> {
        if self.page.offset.as_usize() == 0 && self.append_pos.as_usize() == 0 {
            self.append_pos = self.len()?;
            self.page = Page::new(self.append_pos);
            if self.append_pos.as_usize() == 0 {
                self.append_slice(&[0xBC,0xDA])?;
            }
        }
        Ok(())
    }

    pub fn shutdown (&mut self) {
        self.async_file.shutdown()
    }

    fn page_iter (&self, pagenumber: usize) -> PageIterator {
        PageIterator::new(self, pagenumber)
    }

    pub fn data_iter (&self) -> DataIterator {
        DataIterator::new(self.page_iter(0))
    }

    pub fn get (&self, offset: Offset) -> Result<Option<DataEntry>, BCSError> {
        let page = self.read_page(offset.this_page())?;
        let mut fetch_iterator = DataIterator::new_fetch(
            PageIterator::new(self, offset.page_number()+1), offset.in_page_pos(), page);
        return Ok(fetch_iterator.next());
    }

    pub fn get_spillover (&self, offset: Offset) -> Result<(Offset, Offset), BCSError> {
        let page = self.read_page(offset.this_page())?;
        let mut buf = [0u8; 12];
        page.read(offset.in_page_pos(), &mut buf)?;
        Ok((Offset::from_slice(&buf[..6])?, Offset::from_slice(&buf[6..])?))
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
        U24::new(KEY_LEN + entry.data.len())?.serialize(&mut len);
        self.append_slice(&len)?;
        self.append_slice(entry.data_key.as_slice())?;
        self.append_slice(entry.data.as_slice())?;
        return Ok(start);
    }

    fn append_slice (&mut self, slice: &[u8]) -> Result<(), BCSError> {
        let mut wrote = 0;
        let mut pos = self.append_pos.in_page_pos();
        while wrote < slice.len() {
            if pos == PAYLOAD_MAX {
                self.async_file.append_page(Arc::new(self.page.clone()));
                self.append_pos = self.append_pos.next_page()?;
                self.page = Page::new (self.append_pos);
                pos = 0;
            }
            let have = min(slice.len() - wrote, PAYLOAD_MAX - pos);
            self.page.payload [pos .. pos + have].copy_from_slice (&slice[wrote .. wrote + have]);
            pos += have;
            wrote += have;
        }
        self.append_pos = Offset::new(self.append_pos.as_usize() + wrote)?;
        Ok(())
    }
}

impl DBFile for DataFile {
    fn flush(&mut self) -> Result<(), BCSError> {
        if self.append_pos.in_page_pos() > 0 {
            self.async_file.append_page(Arc::new(self.page.clone()));
        }
        self.async_file.flush()
    }

    fn sync(&mut self) -> Result<(), BCSError> {
        self.async_file.sync()
    }

    fn truncate(&mut self, offset: Offset) -> Result<(), BCSError> {
        self.async_file.truncate(offset)
    }

    fn len(&mut self) -> Result<Offset, BCSError> {
        self.async_file.len()
    }
}

impl PageFile for DataFile {
    fn read_page(&self, offset: Offset) -> Result<Arc<Page>, BCSError> {
        self.async_file.read_page(offset)
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
    TableSpillOver
}

impl DataType {
    pub fn from (data_type: u8) -> DataType {
        match data_type {
            1 => DataType::AppData,
            2 => DataType::TableSpillOver,
            _ => DataType::Padding
        }
    }

    pub fn to_u8 (&self) -> u8 {
        match self {
            DataType::Padding => 0,
            DataType::AppData => 1,
            DataType::TableSpillOver => 2
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
    pub fn new_spillover (offset: Offset, next: Offset) -> DataEntry {
        let mut sp = [0u8; 12];
        offset.serialize(&mut sp[..6]);
        offset.serialize(&mut sp[6..]);
        DataEntry{data_type: DataType::TableSpillOver, data_key: Vec::new(), data: sp.to_vec()}
    }
}

pub struct DataIterator<'file> {
    page_iterator: PageIterator<'file>,
    current: Option<Arc<Page>>,
    pos: usize
}

impl<'file> DataIterator<'file> {
    pub fn new (page_iterator: PageIterator<'file>) -> DataIterator {
        DataIterator{page_iterator, pos: 0, current: None}
    }

    pub fn new_fetch (page_iterator: PageIterator<'file>, pos: usize, page: Arc<Page>) -> DataIterator {
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
                    if data_type == DataType::TableSpillOver {
                        self.pos += 12;
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
                else if data_type == DataType::TableSpillOver {
                    self.pos += 12;
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
        let mut data = DataFile::new(Box::new(mem));
        assert!(data.page_iter(0).next().is_none());
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