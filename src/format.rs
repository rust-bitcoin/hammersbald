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
//! # Content types
//!
use error::BCDBError;
use offset::Offset;
use page::{Page, PAGE_SIZE, PAGE_PAYLOAD_SIZE};
use pagedfile::{PagedFileIterator, PagedFile, FileOps};

use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

use std::io::{Write, Read, Cursor};
use std::cmp::min;

/// Content envelope wrapping payload in data and link files
pub struct Envelope {
    /// length of this entry. Useful for forward iteration
    pub length: u32,
    /// pointer to previous entry. Useful for backward iteration
    pub previous: Offset,
    /// payload
    pub payload: Vec<u8>
}

impl Envelope {
    /// serialize for storage
    pub fn serialize (&self, result: &mut Write) {
        let length = (self.payload.len() + 9) as u32;
        result.write_u24::<BigEndian>(length).unwrap();
        result.write_u48::<BigEndian>(self.previous.as_u64()).unwrap();
        result.write(self.payload.as_slice()).unwrap();
    }

    /// deserialize from storage
    pub fn deserialize(reader: &mut Read) -> Result<Envelope, BCDBError> {
        let length = reader.read_u24::<BigEndian>()?;
        let previous = Offset::from(reader.read_u48::<BigEndian>()?);
        let mut payload = vec!(0u8; length as usize - 9);
        reader.read(&mut payload)?;
        Ok(Envelope{length, previous, payload})
    }
}

/// all available payloads
pub enum Payload {
    /// payload that carries IndexedData
    Indexed(IndexedData),
    /// payload that carries OwnedData
    Referred(Data),
    /// payload that carries a Link
    Link(Link),
    /// payload thay carries a Table entry
    Table(Offset)
}

impl Payload {
    /// serialize for storage
    pub fn serialize (&self, result: &mut Write) {
        match self {
            Payload::Indexed(indexed) => {
                result.write_u8(0).unwrap();
                indexed.serialize(result);
            },
            Payload::Referred(referred) => {
                result.write_u8(1).unwrap();
                referred.serialize(result);
            },
            // Link and Table are not serialized with a type
            _ => {}
        }
    }

    /// deserialize from storage
    pub fn deserialize(reader: &mut Read) -> Result<Payload, BCDBError> {
        match reader.read_u8()? {
            0 => Ok(Payload::Indexed(IndexedData::deserialize(reader)?)),
            1 => Ok(Payload::Referred(Data::deserialize(reader)?)),
            // Link and Table are not serialized with a type
            _ => Err(BCDBError::Corrupted("unknown payload type".to_string()))
        }
    }
}


/// data that is accessible only if its position is known
pub struct Data {
    /// data
    pub data: Vec<u8>,
    /// further accessible data (OwnedData)
    pub referred: Vec<Offset>
}

impl Data {
    /// serialize for storage
    pub fn serialize (&self, result: &mut Write) {
        result.write_u8(self.data.len() as u8).unwrap();
        result.write(self.data.as_slice()).unwrap();
        result.write_u16::<BigEndian>(self.referred.len() as u16).unwrap();
        for offset in &self.referred {
            result.write_u48::<BigEndian>(offset.as_u64()).unwrap();
        }
    }

    /// deserialize from storage
    pub fn deserialize(reader: &mut Read) -> Result<Data, BCDBError> {
        let data_len = reader.read_u8()? as usize;
        let mut data = vec!(0u8; data_len);
        reader.read(data.as_mut_slice())?;

        let owned_len = reader.read_u16::<BigEndian>()? as usize;
        let mut referred = Vec::new();
        for _ in 0 .. owned_len {
            referred.push(Offset::from(reader.read_u48::<BigEndian>()?));
        }
        Ok(Data {data, referred })
    }
}

/// data accessible with a key
pub struct IndexedData {
    /// key
    pub key: Vec<u8>,
    /// data
    pub data: Data
}

impl IndexedData {
    /// serialize for storage
    pub fn serialize (&self, result: &mut Write) {
        result.write_u8(self.key.len() as u8).unwrap();
        result.write(self.key.as_slice()).unwrap();
        self.data.serialize(result);
    }

    /// deserialize from storage
    pub fn deserialize(reader: &mut Read) -> Result<IndexedData, BCDBError> {
        let key_len = reader.read_u8()? as usize;
        let mut key = vec!(0u8; key_len);
        reader.read(key.as_mut_slice())?;

        let data = Data::deserialize(reader)?;
        Ok(IndexedData{key, data })
    }
}

/// A link to IndexedData
pub struct Link {
    /// hash of the key
    pub hash: u32,
    /// data category
    pub cat: u16,
    /// pointer to the Envelope of an IndexedData
    pub envelope: Offset,
    /// pointer to previous link
    pub previous: Offset
}

impl Link {
    /// serialize for storage
    pub fn serialize (&self, result: &mut Write) {
        result.write_u32::<BigEndian>(self.hash).unwrap();
        result.write_u16::<BigEndian>(self.cat).unwrap();
        result.write_u48::<BigEndian>(self.envelope.as_u64()).unwrap();
        result.write_u48::<BigEndian>(self.previous.as_u64()).unwrap();
    }

    /// deserialize from storage
    pub fn deserialize(reader: &mut Read) -> Result<Link, BCDBError> {
        let hash = reader.read_u32::<BigEndian>()?;
        let cat = reader.read_u16::<BigEndian>()?;
        let envelope = Offset::from(reader.read_u48::<BigEndian>()?);
        let previous = Offset::from(reader.read_u48::<BigEndian>()?);
        Ok(Link{hash, cat, envelope, previous})
    }
}

/// Formatter for PagedFile
pub struct Formatter {
    file: Box<PagedFile>,
    page: Option<Page>,
    page_offset: Offset,
    append_pos: Offset,
}

impl Formatter {
    /// create a new formatter for a file
    pub fn new (file: Box<PagedFile>, start: Offset) -> Result<Formatter, BCDBError> {
        Ok(Formatter {file, page: None, page_offset: start.this_page(), append_pos: start })
    }

    /// append a slice at current position
    pub fn append_slice(&mut self, payload: &[u8], lep: Offset) -> Result<(), BCDBError> {
        let mut wrote = 0;
        while wrote < payload.len() {
            let pos = self.append_pos.in_page_pos();
            if self.page.is_none() {
                self.page = Some(self.file.read_page(self.page_offset)?.unwrap_or(Page::new(lep)));
            }
            if let Some(ref mut page) = self.page {
                let space = min(PAGE_PAYLOAD_SIZE - pos, payload.len() - wrote);
                page.write(pos, &payload[wrote .. wrote + space]);
                wrote += space;
                self.append_pos += space as u64;
                if self.append_pos.in_page_pos() == PAGE_PAYLOAD_SIZE {
                    page.write_offset(PAGE_PAYLOAD_SIZE, lep);
                    self.append_pos += 6;
                    self.file.append_page(page.clone())?;
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

    /// read a slice of data
    pub fn get_slice (&self, offset: Offset, length: u64) -> Result<Option<Vec<u8>>, BCDBError> {
        // TODO : error propagation
        Ok(ForwardSliceIterator::new(self.file.as_ref(), offset, length).next())
    }

    /// return next append position
    pub fn position (&self) -> Offset {
        self.append_pos
    }
}

impl FileOps for Formatter {
    fn flush(&mut self) -> Result<(), BCDBError> {
        if self.append_pos.in_page_pos() > 0 {
            if let Some(page) = self.page.clone() {
                self.file.append_page(page)?;
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

    fn shutdown(&mut self) {
        self.file.shutdown()
    }
}

/// a formatter for the data file
pub struct DataFormatter {
    formatter: Formatter,
    previous: Offset
}

impl DataFormatter {
    /// create a new formatter
    pub fn new(file: Box<PagedFile>, start: Offset, previous: Offset) -> Result<DataFormatter, BCDBError> {
        Ok(DataFormatter { formatter: Formatter::new(file, start)?, previous })
    }

    /// append a slice at current position
    pub fn append_slice(&mut self, payload: &[u8], lep: Offset) -> Result<(), BCDBError> {
        self.formatter.append_slice(payload, lep)
    }


    /// get a stored content at offset
    pub fn get_payload(&self, offset: Offset) -> Result<Option<Payload>, BCDBError> {
        // TODO: propagate errors from next()
        if let Some(envelope) = ForwardEnvelopeIterator::new(&self.formatter, offset).next() {
            return Ok(Some(Payload::deserialize(&mut Cursor::new(envelope.payload.as_slice()))?));
        }
        Ok(None)
    }

    /// append data
    pub fn append_payload (&mut self, payload: Payload) -> Result<Offset, BCDBError> {
        let mut content = Vec::new();
        payload.serialize(&mut content);
        let mut envelope = Vec::new();
        Envelope{length: content.len() as u32 + 9, previous: self.previous, payload: content}.serialize(&mut envelope);
        let me = self.formatter.position();
        self.formatter.append_slice(envelope.as_slice(), me)?;
        self.previous = me;
        Ok(me)
    }
}

impl FileOps for DataFormatter {
    fn flush(&mut self) -> Result<(), BCDBError> {
        self.formatter.flush()
    }

    fn len(&self) -> Result<u64, BCDBError> {
        self.formatter.len()
    }

    fn truncate(&mut self, new_len: u64) -> Result<(), BCDBError> {
        self.formatter.truncate(new_len)
    }

    fn sync(&self) -> Result<(), BCDBError> {
        self.formatter.sync()
    }

    fn shutdown(&mut self) {
        self.formatter.shutdown()
    }
}

/// An iterator returning data in offset ascending order
pub struct ForwardEnvelopeIterator<'file> {
    reader: &'file Formatter,
    start: Offset
}

impl<'file> ForwardEnvelopeIterator<'file> {
    /// create a new iterator returning envelopes in offset ascending order
    pub fn new (reader: &'file Formatter, start: Offset) -> ForwardEnvelopeIterator<'file> {
        ForwardEnvelopeIterator {reader, start}
    }
}

impl<'file> Iterator for ForwardEnvelopeIterator<'file> {
    type Item = Envelope;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        if let Some(header) = self.reader.get_slice(self.start, 9).unwrap() {
            let mut cursor = Cursor::new(header);
            let length = cursor.read_u24::<BigEndian>().unwrap() as u64;
            let previous = Offset::from(cursor.read_u48::<BigEndian>().unwrap());
            if let Some(payload) = self.reader.get_slice(self.start + 9, length - 9).unwrap() {
                self.start += length;
                return Some(Envelope{length: length as u32, previous, payload})
            }
        }
        None
    }
}

/// An iterator returning data in offset descending order
pub struct BackwardEnvelopeIterator<'file> {
    reader: &'file Formatter,
    start: Offset,
    fence: Offset
}

impl<'file> BackwardEnvelopeIterator<'file> {
    /// create a new iterator returning data in offset ascending order
    /// iteration is limited to [fence .. start]
    pub fn new (reader: &'file Formatter, start: Offset, fence: Offset) -> BackwardEnvelopeIterator<'file> {
        BackwardEnvelopeIterator { reader, start, fence}
    }
}

impl<'file> Iterator for BackwardEnvelopeIterator<'file> {
    type Item = Envelope;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        if self.reader.position() < self.fence {
            return None;
        }
        if let Some(header) = self.reader.get_slice(self.start, 9).unwrap() {
            let mut cursor = Cursor::new(header);
            let length = cursor.read_u24::<BigEndian>().unwrap() as u64;
            let previous = Offset::from(cursor.read_u48::<BigEndian>().unwrap());
            if let Some(payload) = self.reader.get_slice(self.start + 9, length - 9).unwrap() {
                self.start = previous;
                return Some(Envelope { length: length as u32, previous, payload })
            }
        }
        None
    }
}


/// An iterator returning uniform length slices of data in offset ascending order
pub struct ForwardSliceIterator<'file> {
    length: u64,
    reader: PagedFileIterator<'file>
}

impl<'file> ForwardSliceIterator<'file> {
    /// create a new iterator returning uniform length slices of data in offset ascending order
    pub fn new (file: &'file PagedFile, start: Offset, length: u64) -> ForwardSliceIterator<'file> {
        ForwardSliceIterator {reader: PagedFileIterator::new(file, start), length}
    }
}

impl<'file> Iterator for ForwardSliceIterator<'file> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        let mut buf = vec!(0u8; self.length as usize);
        let length = self.reader.read(&mut buf).unwrap();
        if length > 0 {
            return Some(buf);
        }
        None
    }
}


/// An iterator returning envelopes in offset descending order
pub struct BackwardSliceIterator<'file> {
    length: u64,
    file: &'file PagedFile,
    reader: PagedFileIterator<'file>,
    fence: Offset
}

impl<'file> BackwardSliceIterator<'file> {
    /// create a new iterator returning envelopes in offset ascending order
    pub fn new (file: &'file PagedFile, start: Offset, fence: Offset, length: u64) -> BackwardSliceIterator<'file> {
        BackwardSliceIterator{file, reader: PagedFileIterator::new(file, start), fence, length}
    }
}

impl<'file> Iterator for BackwardSliceIterator<'file> {
    type Item = Envelope;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        if self.reader.position () < self.fence {
            return None;
        }
        let mut buf = vec!(0u8; self.length as usize);
        self.reader.read(&mut buf).unwrap();
        self.reader = PagedFileIterator::new(self.file, self.reader.position() - self.length);
        Some(Envelope::deserialize(&mut Cursor::new(&buf)).unwrap())
    }
}


