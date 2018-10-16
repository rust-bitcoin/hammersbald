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
use page::{Page, PAGE_SIZE};
use pagedfile::{PagedFileIterator, PagedFile};

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
    Owned(OwnedData),
    /// payload that carries a LinkChain
    Link(Link)
}

impl Payload {
    /// serialize for storage
    pub fn serialize (&self, result: &mut Write) {
        match self {
            Payload::Indexed(indexed) => {
                result.write_u8(0).unwrap();
                indexed.serialize(result);
            },
            Payload::Owned(owned) => {
                result.write_u8(1).unwrap();
                owned.serialize(result);
            },
            Payload::Link(link) => {
                result.write_u8(2).unwrap();
                link.serialize(result);
            }
        }
    }

    /// deserialize from storage
    pub fn deserialize(reader: &mut Read) -> Result<Payload, BCDBError> {
        match reader.read_u8()? {
            0 => Ok(Payload::Indexed(IndexedData::deserialize(reader)?)),
            1 => Ok(Payload::Owned(OwnedData::deserialize(reader)?)),
            2 => Ok(Payload::Link(Link::deserialize(reader)?)),
            _ => Err(BCDBError::Corrupted("unknown payload type".to_string()))
        }
    }
}

/// data accessible with a key
pub struct IndexedData {
    /// key
    pub key: Vec<u8>,
    /// data
    pub data: Vec<u8>,
    /// further accessible data (OwnedData)
    pub owned: Vec<Offset>
}

impl IndexedData {
    /// serialize for storage
    pub fn serialize (&self, result: &mut Write) {
        result.write_u8(self.key.len() as u8).unwrap();
        result.write(self.key.as_slice()).unwrap();
        result.write_u8(self.data.len() as u8).unwrap();
        result.write(self.data.as_slice()).unwrap();
        result.write_u16::<BigEndian>(self.owned.len() as u16).unwrap();
        for offset in &self.owned {
            result.write_u48::<BigEndian>(offset.as_u64()).unwrap();
        }
    }

    /// deserialize from storage
    pub fn deserialize(reader: &mut Read) -> Result<IndexedData, BCDBError> {
        let key_len = reader.read_u8()? as usize;
        let mut key = vec!(0u8; key_len);
        reader.read(key.as_mut_slice())?;

        let data_len = reader.read_u8()? as usize;
        let mut data = vec!(0u8; data_len);
        reader.read(data.as_mut_slice())?;

        let owned_len = reader.read_u16::<BigEndian>()? as usize;
        let mut owned = Vec::new();
        for _ in 0 .. owned_len {
            owned.push(Offset::from(reader.read_u48::<BigEndian>()?));
        }
        Ok(IndexedData{key, data, owned})
    }
}

/// data that is indirectly accessible through keyed data
pub struct OwnedData {
    /// data
    pub data: Vec<u8>,
    /// further accessible data (OwnedData)
    pub owned: Vec<Offset>
}

impl OwnedData {
    /// serialize for storage
    pub fn serialize (&self, result: &mut Write) {
        result.write_u8(self.data.len() as u8).unwrap();
        result.write(self.data.as_slice()).unwrap();
        result.write_u16::<BigEndian>(self.owned.len() as u16).unwrap();
        for offset in &self.owned {
            result.write_u48::<BigEndian>(offset.as_u64()).unwrap();
        }
    }

    /// deserialize from storage
    pub fn deserialize(reader: &mut Read) -> Result<OwnedData, BCDBError> {
        let data_len = reader.read_u8()? as usize;
        let mut data = vec!(0u8; data_len);
        reader.read(data.as_mut_slice())?;

        let owned_len = reader.read_u16::<BigEndian>()? as usize;
        let mut owned = Vec::new();
        for _ in 0 .. owned_len {
            owned.push(Offset::from(reader.read_u48::<BigEndian>()?));
        }
        Ok(OwnedData{data, owned})
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

/// An Envelope writer for PagedFile
pub struct EnvelopeAppender<'file> {
    appender: SliceAppender<'file>,
    previous: Offset,
}

impl<'file> EnvelopeAppender<'file> {
    /// create a new envelope appender for a file
    /// envelope will be appended at next
    /// assumes that previous is the offset of the last appended envelope
    pub fn new (file: &'file mut PagedFile, current: Offset, previous: Offset) -> EnvelopeAppender<'file> {
        EnvelopeAppender {appender:
            SliceAppender{file, page: None, pos: current.in_page_pos(), page_offset: current.this_page(), current},
            previous}
    }

    /// wrap some data into an envelope and append to the file
    /// returns the offset for next append
    pub fn append(&mut self, payload: &[u8]) -> Result<Offset, BCDBError> {
        let mut header = Vec::with_capacity(9);
        header.write_u24::<BigEndian>(payload.len() as u32 + 9)?;
        header.write_u48::<BigEndian>(self.previous.as_u64())?;
        self.appender.append_slice(header.as_slice())?;
        self.appender.append_slice(payload)?;
        Ok(self.appender.position())
    }

    /// extend with contents from an iterator
    pub fn extend(&mut self, mut from: impl Iterator<Item=Vec<u8>>) -> Result<Offset, BCDBError> {
        while let Some(payload) = from.next() {
            let next = self.appender.position();
            self.append(payload.as_slice())?;
            self.previous = next;
        }
        Ok(self.appender.position())
    }
}

/// An iterator returning envelopes in offset ascending order
pub struct ForwardEnvelopeIterator<'file> {
    reader: PagedFileIterator<'file>,
    fence: Offset
}

impl<'file> ForwardEnvelopeIterator<'file> {
    /// create a new iterator returning envelopes in offset ascending order
    /// iteration is limited to [start .. fence]
    pub fn new (file: &'file PagedFile, start: Offset, fence: Offset) -> ForwardEnvelopeIterator<'file> {
        ForwardEnvelopeIterator{reader: PagedFileIterator::new(file, start), fence}
    }
}

impl<'file> Iterator for ForwardEnvelopeIterator<'file> {
    type Item = Envelope;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        if self.reader.position() >= self.fence {
            return None;
        }
        let length = self.reader.read_u24::<BigEndian>().unwrap() as usize;
        let mut buf = vec!(0u8; length - 3);
        self.reader.read(&mut buf).unwrap();
        Some(Envelope::deserialize(&mut Cursor::new(&buf[6..])).unwrap())
    }
}

/// An iterator returning envelopes in offset descending order
pub struct BackwardEnvelopeIterator<'file> {
    file: &'file PagedFile,
    reader: PagedFileIterator<'file>,
    fence: Offset
}

impl<'file> BackwardEnvelopeIterator<'file> {
    /// create a new iterator returning envelopes in offset ascending order
    /// iteration is limited to [fence .. start]
    pub fn new (file: &'file PagedFile, start: Offset, fence: Offset) -> BackwardEnvelopeIterator<'file> {
        BackwardEnvelopeIterator{file, reader: PagedFileIterator::new(file, start), fence}
    }
}

impl<'file> Iterator for BackwardEnvelopeIterator<'file> {
    type Item = Envelope;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        if self.reader.position() < self.fence {
            return None;
        }
        let length = self.reader.read_u24::<BigEndian>().unwrap() as usize;
        let previous = Offset::from(self.reader.read_u48::<BigEndian>().unwrap());
        let mut buf = vec!(0u8; length - 9);
        self.reader.read(&mut buf).unwrap();
        self.reader = PagedFileIterator::new(self.file, previous);
        Some(Envelope::deserialize(&mut Cursor::new(&buf)).unwrap())
    }
}

/// An Envelope writer for PagedFile
pub struct SliceAppender<'file> {
    file: &'file mut PagedFile,
    page: Option<Page>,
    pos: usize,
    page_offset: Offset,
    current: Offset,
}

impl<'file> SliceAppender<'file> {
    /// create a new slice appender for a file
    pub fn new (file: &'file mut PagedFile, current: Offset) -> SliceAppender<'file> {
        SliceAppender {file, page: None, pos: current.in_page_pos(), page_offset: current.this_page(), current }
    }

    /// append a slice at current position
    pub fn append_slice(&mut self, payload: &[u8]) -> Result<(), BCDBError> {
        let mut wrote = 0;
        while wrote < payload.len() {
            if self.page.is_none() {
                self.page = Some(self.file.read_page(self.page_offset)?.unwrap_or(Page::new()));
                self.pos = 0;
            }
            if let Some(ref mut page) = self.page {
                let space = min(PAGE_SIZE - self.pos, payload.len() - wrote);
                page.payload[self.pos .. self.pos + space].copy_from_slice(&payload[wrote .. wrote + space]);
                wrote += space;
                self.pos += space;
                if self.pos == PAGE_SIZE {
                    self.file.write_page(self.page_offset, page.clone())?;
                }
            }
            if self.pos == PAGE_SIZE {
                self.page_offset = Offset::from(self.page_offset.as_u64() + PAGE_SIZE as u64);
                self.page = None;
                self.pos = 0;
            }
        }
        self.current += payload.len() as u64;
        Ok(())
    }

    /// return next append position
    pub fn position (&self) -> Offset {
        self.current
    }

    /// extend with contents from an iterator
    pub fn extend(&mut self, mut from: impl Iterator<Item=Vec<u8>>) -> Result<Offset, BCDBError> {
        while let Some(payload) = from.next() {
            self.append_slice(payload.as_slice())?;
        }
        Ok(self.current)
    }
}

/// An iterator returning uniform length slices of data in offset ascending order
pub struct ForwardSliceIterator<'file> {
    length: u64,
    reader: PagedFileIterator<'file>,
    fence: Offset
}

impl<'file> ForwardSliceIterator<'file> {
    /// create a new iterator returning uniform length slices of data in offset ascending order
    /// iteration is limited to [start .. fence]
    pub fn new (file: &'file PagedFile, start: Offset, fence: Offset, length: u64) -> ForwardSliceIterator<'file> {
        ForwardSliceIterator {reader: PagedFileIterator::new(file, start), fence, length}
    }
}

impl<'file> Iterator for ForwardSliceIterator<'file> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        if self.reader.position() >= self.fence {
            return None;
        }
        let mut buf = vec!(0u8; self.length as usize);
        self.reader.read(&mut buf).unwrap();
        Some(buf)
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


