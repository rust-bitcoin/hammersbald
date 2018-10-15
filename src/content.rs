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
    pub fn serialize (&self) -> Vec<u8> {
        let payload = self.payload.serialize();
        let length = (payload.len() + 9) as u32;
        let mut result = Vec::new();
        result.write_u24::<BigEndian>(length).unwrap();
        result.write_u48::<BigEndian>(self.previous.as_u64()).unwrap();
        result.extend(payload);
        result
    }

    /// deserialize from storage
    pub fn deserialize(cursor: &mut Cursor<&[u8]>) -> Result<Envelope, BCDBError> {
        let length = cursor.read_u24::<BigEndian>()?;
        let previous = Offset::from(cursor.read_u48::<BigEndian>()?);
        Ok(Envelope{length, previous, payload: Payload::deserialize(cursor)?})
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
    pub fn serialize (&self) -> Vec<u8> {
        let mut result = Vec::new();
        match self {
            Payload::Indexed(indexed) => {
                result.write_u8(0).unwrap();
                result.extend(indexed.serialize())
            },
            Payload::Owned(owned) => {
                result.write_u8(1).unwrap();
                result.extend(owned.serialize())
            },
            Payload::Chain(chain) => {
                result.write_u8(2).unwrap();
                result.extend(chain.serialize())
            }
        }
        result
    }

    /// deserialize from storage
    pub fn deserialize(cursor: &mut Cursor<&[u8]>) -> Result<Payload, BCDBError> {
        match cursor.read_u8()? {
            0 => Ok(Payload::Indexed(IndexedData::deserialize(cursor)?)),
            1 => Ok(Payload::Owned(OwnedData::deserialize(cursor)?)),
            2 => Ok(Payload::Chain(LinkChain::deserialize(cursor)?)),
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
    pub fn serialize (&self) -> Vec<u8> {
        let mut result = Vec::new();
        result.write_u8(self.key.len() as u8).unwrap();
        result.write(self.key.as_slice()).unwrap();
        result.write_u8(self.data.len() as u8).unwrap();
        result.write(self.data.as_slice()).unwrap();
        result.write_u16::<BigEndian>(self.owned.len() as u16).unwrap();
        for offset in &self.owned {
            result.write_u48::<BigEndian>(offset.as_u64()).unwrap();
        }
        result
    }

    /// deserialize from storage
    pub fn deserialize(cursor: &mut Cursor<&[u8]>) -> Result<IndexedData, BCDBError> {
        let key_len = cursor.read_u8()? as usize;
        let mut key = vec!(0u8; key_len);
        cursor.read(key.as_mut_slice())?;

        let data_len = cursor.read_u8()? as usize;
        let mut data = vec!(0u8; data_len);
        cursor.read(data.as_mut_slice())?;

        let owned_len = cursor.read_u16::<BigEndian>()? as usize;
        let mut owned = Vec::new();
        for _ in 0 .. owned_len {
            owned.push(Offset::from(cursor.read_u48::<BigEndian>()?));
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
    pub fn serialize (&self) -> Vec<u8> {
        let mut result = Vec::new();
        result.write_u8(self.data.len() as u8).unwrap();
        result.write(self.data.as_slice()).unwrap();
        result.write_u16::<BigEndian>(self.owned.len() as u16).unwrap();
        for offset in &self.owned {
            result.write_u48::<BigEndian>(offset.as_u64()).unwrap();
        }
        result
    }

    /// deserialize from storage
    pub fn deserialize(cursor: &mut Cursor<&[u8]>) -> Result<OwnedData, BCDBError> {
        let data_len = cursor.read_u8()? as usize;
        let mut data = vec!(0u8; data_len);
        cursor.read(data.as_mut_slice())?;

        let owned_len = cursor.read_u16::<BigEndian>()? as usize;
        let mut owned = Vec::new();
        for _ in 0 .. owned_len {
            owned.push(Offset::from(cursor.read_u48::<BigEndian>()?));
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
    pub fn serialize (&self) -> Vec<u8> {
        let mut result = Vec::new();
        result.write_u32::<BigEndian>(self.hash).unwrap();
        result.write_u48::<BigEndian>(self.envelope.as_u64()).unwrap();
        result
    }

    /// deserialize from storage
    pub fn deserialize(cursor: &mut Cursor<&[u8]>) -> Result<Link, BCDBError> {
        let hash = cursor.read_u32::<BigEndian>()?;
        let envelope = Offset::from(cursor.read_u48::<BigEndian>()?);
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
    pub fn serialize (&self) -> Vec<u8> {
        let mut result = Vec::new();
        result.extend(self.link.serialize());
        result.write_u48::<BigEndian>(self.previous.as_u64()).unwrap();
        result
    }

    /// deserialize from storage
    pub fn deserialize(cursor: &mut Cursor<&[u8]>) -> Result<LinkChain, BCDBError> {
        let link = Link::deserialize(cursor)?;
        let previous = Offset::from(cursor.read_u48::<BigEndian>()?);
        Ok(LinkChain{link, previous})
    }
}
