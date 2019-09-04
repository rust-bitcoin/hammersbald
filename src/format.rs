//
// Copyright 2018-2019 Tamas Blummer
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
use error::Error;
use pref::PRef;

use byteorder::{WriteBytesExt, ByteOrder, BigEndian};

use std::io::Write;

/// Content envelope wrapping in data file
pub struct Envelope {
    buffer: Vec<u8>
}

impl Envelope {
    /// create a new envelope
    pub fn new (payload: &[u8]) -> Envelope {
        Envelope{buffer: payload.to_vec()}
    }

    /// envelope payload
    pub fn payload (&self) -> &[u8] {
        self.buffer.as_slice()
    }

    /// serialize for storage
    pub fn serialize (&self, result: &mut dyn Write) {
        result.write_u24::<BigEndian>(self.buffer.len() as u32).unwrap();
        result.write(self.buffer.as_slice()).unwrap();
    }

    /// deserialize for storage
    pub fn deseralize(buffer: Vec<u8>) -> Envelope {
        Envelope{buffer}
    }
}

/// payloads in the data file
pub enum Payload<'e> {
    /// indexed data
    Indexed(IndexedData<'e>),
    /// data
    Referred(Data<'e>),
    /// hash table extension,
    Link(Link<'e>)
}

impl<'e> Payload<'e> {
    /// serialize for storage
    pub fn serialize (&self, result: &mut dyn Write) {
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
    pub fn deserialize(slice: &'e [u8]) -> Result<Payload, Error> {
        match slice [0] {
            0 => Ok(Payload::Indexed(IndexedData::deserialize(&slice[1..]))),
            1 => Ok(Payload::Referred(Data::deserialize(&slice[1..]))),
            2 => Ok(Payload::Link(Link::deserialize(&slice[1..]))),
            // Link and Table are not serialized with a type
            _ => Err(Error::Corrupted("unknown payload type".to_string()))
        }
    }
}


/// data that is accessible only if its position is known
pub struct Data<'e> {
    /// data
    pub data: &'e [u8],
}

impl<'e> Data<'e> {
    /// create new data
    pub fn new(data: &'e [u8]) -> Data<'e> {
        Data { data }
    }

    /// serialize for storage
    pub fn serialize (&self, result: &mut dyn Write) {
        result.write_u24::<BigEndian>(self.data.len() as u32).unwrap();
        result.write(self.data).unwrap();
    }

    /// deserialize from storage
    pub fn deserialize(slice: &'e [u8]) -> Data {
        let data_len = BigEndian::read_u24(&slice[0 .. 3]) as usize;
        let data = &slice[3 .. 3+data_len];
        Data {data}
    }
}

/// data accessible with a key
pub struct IndexedData<'e> {
    /// key
    pub key: &'e [u8],
    /// data
    pub data: Data<'e>
}

impl<'e> IndexedData<'e> {
    /// new indexed data
    pub fn new (key: &'e [u8], data: Data<'e>) -> IndexedData<'e> {
        IndexedData {key, data}
    }

    /// serialize for storage
    pub fn serialize (&self, result: &mut dyn Write) {
        result.write_u8(self.key.len() as u8).unwrap();
        result.write(self.key).unwrap();
        self.data.serialize(result);
    }

    /// deserialize from storage
    pub fn deserialize(slice: &'e [u8]) -> IndexedData<'e> {
        let key_len = slice[0] as usize;
        let key = &slice[1 .. key_len+1];
        let data = Data::deserialize(&slice[key_len+1 ..]);
        IndexedData{key, data }
    }
}

/// A link to data
pub struct Link<'e> {
    /// slots
    links: &'e [u8]
}

impl<'e> Link<'e> {
    /// serialize slots
    pub fn from_slots(slots: &[(u32, PRef)]) -> Vec<u8> {
        let mut links = vec!(0u8;10*slots.len());
        for (i, slot) in slots.iter().enumerate() {
            BigEndian::write_u32(&mut links[i*10 .. i*10+4], slot.0);
            BigEndian::write_u48(&mut links[i*10+4 .. i*10+10], slot.1.as_u64());
        }
        links
    }

    /// get slots
    pub fn slots(&self) -> Vec<(u32, PRef)> {
        let mut slots = vec!();
        for i in 0 .. self.links.len()/10 {
            let hash = BigEndian::read_u32(&self.links[i*10..i*10+4]);
            let pref = PRef::from(BigEndian::read_u48(&self.links[i*10+4..i*10+10]));
            slots.push((hash, pref));
        }
        slots
    }

    /// serialize for storage
    pub fn serialize (&self, write: &mut dyn Write) {
        write.write(&self.links).unwrap();
    }

    /// deserialize from storage
    pub fn deserialize(slice: &'e [u8]) -> Link<'e> {
        Link{links: slice}
    }
}
