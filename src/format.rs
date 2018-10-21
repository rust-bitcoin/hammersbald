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
use pref::PRef;

use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

use std::io::{Write, Read};

/// Content envelope wrapping in data file
pub struct Envelope {
    /// pointer to previous entry. Useful for backward iteration
    pub previous: PRef,
    /// payload
    pub payload: Payload
}

impl Envelope {
    /// serialize for storage
    pub fn serialize (&self, result: &mut Write) {
        result.write_u48::<BigEndian>(self.previous.as_u64()).unwrap();
        let mut payload = vec!();
        self.payload.serialize(&mut payload);
        result.write(payload.as_slice()).unwrap();

    }

    /// deserialize for storage
    pub fn deseralize(reader: &mut Read) -> Result<Envelope, BCDBError> {
        let previous = PRef::from(reader.read_u48::<BigEndian>()?);
        Ok(Envelope{payload: Payload::deserialize(reader)?, previous})
    }
}

/// payloads in the data file
pub enum Payload {
    /// indexed data
    Indexed(IndexedData),
    /// data
    Referred(Data),
    /// hash table extension,
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
            Payload::Referred(referred) => {
                result.write_u8(1).unwrap();
                referred.serialize(result);
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
            1 => Ok(Payload::Referred(Data::deserialize(reader)?)),
            2 => Ok(Payload::Link(Link::deserialize(reader)?)),
            // Link and Table are not serialized with a type
            _ => Err(BCDBError::Corrupted("unknown payload type".to_string()))
        }
    }
}


/// data that is accessible only if its position is known
pub struct Data {
    /// data
    pub data: Vec<u8>,
    /// further accessible data
    pub referred: Vec<PRef>
}

impl Data {
    /// serialize for storage
    pub fn serialize (&self, result: &mut Write) {
        result.write_u24::<BigEndian>(self.data.len() as u32).unwrap();
        result.write(self.data.as_slice()).unwrap();
        result.write_u16::<BigEndian>(self.referred.len() as u16).unwrap();
        for pref in &self.referred {
            result.write_u48::<BigEndian>(pref.as_u64()).unwrap();
        }
    }

    /// deserialize from storage
    pub fn deserialize(reader: &mut Read) -> Result<Data, BCDBError> {
        let data_len = reader.read_u24::<BigEndian>()? as usize;
        let mut data = vec!(0u8; data_len);
        reader.read(data.as_mut_slice())?;

        let owned_len = reader.read_u16::<BigEndian>()? as usize;
        let mut referred = Vec::new();
        for _ in 0 .. owned_len {
            referred.push(PRef::from(reader.read_u48::<BigEndian>()?));
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

/// A link to data
pub struct Link {
    /// slots
    pub links: Vec<(u32, PRef)>,
}

impl Link {
    /// serialize for storage
    pub fn serialize (&self, result: &mut Write) {
        result.write_u8(self.links.len() as u8).unwrap();
        for (hash, envelope) in &self.links {
            result.write_u32::<BigEndian>(*hash).unwrap();
            result.write_u48::<BigEndian>(envelope.as_u64()).unwrap();
        }
    }

    /// deserialize from storage
    pub fn deserialize(reader: &mut Read) -> Result<Link, BCDBError> {
        let len = reader.read_u8()?;
        let mut links = vec!();
        for _ in 0 .. len {
            let hash = reader.read_u32::<BigEndian>()?;
            let envelope = PRef::from(reader.read_u48::<BigEndian>()?);
            links.push((hash, envelope));
        }

        Ok(Link{links})
    }
}

