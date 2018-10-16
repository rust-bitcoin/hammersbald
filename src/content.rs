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
use pagedfile::{PagedFileIterator, PagedFile};

use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

use std::io::{Write, Read, Cursor};

/// Content envelope wrapping payload in data and link files
pub struct Envelope {
    /// length of this entry. Useful for forward iteration
    pub length: u32,
    /// pointer to previous entry. Useful for backward iteration
    pub previous: Offset,
    /// payload
    pub payload: Payload
}

impl Envelope {
    /// serialize for storage
    pub fn serialize (&self, result: &mut Write) {
        let mut payload = Vec::new();
        self.payload.serialize(&mut payload);
        let length = (payload.len() + 9) as u32;
        result.write_u24::<BigEndian>(length).unwrap();
        result.write_u48::<BigEndian>(self.previous.as_u64()).unwrap();
        result.write(payload.as_slice()).unwrap();
    }

    /// deserialize from storage
    pub fn deserialize(reader: &mut Read) -> Result<Envelope, BCDBError> {
        let length = reader.read_u24::<BigEndian>()?;
        let previous = Offset::from(reader.read_u48::<BigEndian>()?);
        Ok(Envelope{length, previous, payload: Payload::deserialize(reader)?})
    }
}

/// all available payloads
pub enum Payload {
    /// payload that carries IndexedData
    Indexed(IndexedData),
    /// payload that carries OwnedData
    Owned(OwnedData),
    /// payload that carries a LinkChain
    Chain(LinkChain)
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
            Payload::Chain(chain) => {
                result.write_u8(2).unwrap();
                chain.serialize(result);
            }
        }
    }

    /// deserialize from storage
    pub fn deserialize(reader: &mut Read) -> Result<Payload, BCDBError> {
        match reader.read_u8()? {
            0 => Ok(Payload::Indexed(IndexedData::deserialize(reader)?)),
            1 => Ok(Payload::Owned(OwnedData::deserialize(reader)?)),
            2 => Ok(Payload::Chain(LinkChain::deserialize(reader)?)),
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
    /// pointer to the Envelope of an IndexedData
    pub envelope: Offset
}

impl Link {
    /// serialize for storage
    pub fn serialize (&self, result: &mut Write) {
        result.write_u32::<BigEndian>(self.hash).unwrap();
        result.write_u48::<BigEndian>(self.envelope.as_u64()).unwrap();
    }

    /// deserialize from storage
    pub fn deserialize(reader: &mut Read) -> Result<Link, BCDBError> {
        let hash = reader.read_u32::<BigEndian>()?;
        let envelope = Offset::from(reader.read_u48::<BigEndian>()?);
        Ok(Link{hash, envelope})
    }
}

/// A chain of links
pub struct LinkChain {
    /// link
    pub link: Link,
    /// previous link
    pub previous: Offset
}

impl LinkChain {
    /// serialize for storage
    pub fn serialize (&self, result: &mut Write) {
        self.link.serialize(result);
        result.write_u48::<BigEndian>(self.previous.as_u64()).unwrap();
    }

    /// deserialize from storage
    pub fn deserialize(reader: &mut Read) -> Result<LinkChain, BCDBError> {
        let link = Link::deserialize(reader)?;
        let previous = Offset::from(reader.read_u48::<BigEndian>()?);
        Ok(LinkChain{link, previous})
    }
}

/// An iterator returning envelopes in offset ascending order
pub struct ForwardEnvelopeIterator<'file> {
    reader: PagedFileIterator<'file>
}

impl<'file> ForwardEnvelopeIterator<'file> {
    /// create a new iterator returning envelopes in offset ascending order
    pub fn new (file: &'file PagedFile, start: Offset) -> ForwardEnvelopeIterator<'file> {
        ForwardEnvelopeIterator{reader: PagedFileIterator::new(file, start)}
    }
}

impl<'file> Iterator for ForwardEnvelopeIterator<'file> {
    type Item = Envelope;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        let length = self.reader.read_u24::<BigEndian>().unwrap() as usize;
        let mut buf = vec!(0u8; length - 3);
        self.reader.read(&mut buf).unwrap();
        Some(Envelope::deserialize(&mut Cursor::new(&buf[6..])).unwrap())
    }
}

/// An iterator returning envelopes in offset descending order
pub struct BackwardEnvelopeIterator<'file> {
    file: &'file PagedFile,
    reader: PagedFileIterator<'file>
}

impl<'file> BackwardEnvelopeIterator<'file> {
    /// create a new iterator returning envelopes in offset ascending order
    pub fn new (file: &'file PagedFile, start: Offset) -> BackwardEnvelopeIterator<'file> {
        BackwardEnvelopeIterator{file, reader: PagedFileIterator::new(file, start)}
    }
}

impl<'file> Iterator for BackwardEnvelopeIterator<'file> {
    type Item = Envelope;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        let length = self.reader.read_u24::<BigEndian>().unwrap() as usize;
        let previous = Offset::from(self.reader.read_u48::<BigEndian>().unwrap());
        let mut buf = vec!(0u8; length - 9);
        self.reader.read(&mut buf).unwrap();
        self.reader = PagedFileIterator::new(self.file, previous);
        Some(Envelope::deserialize(&mut Cursor::new(&buf)).unwrap())
    }
}
