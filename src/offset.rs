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
//! # Offset type
//! an unsigned 48 bit integer used as file offset
//!

use error::BCSError;

const MAX_VALUE: usize = 1 << 47;

#[derive(Eq, PartialEq, Hash, Copy, Clone, Default)]
pub struct Offset(usize);

impl Offset {
    pub fn new (value: usize) ->Result<Offset, BCSError> {
        if value > MAX_VALUE {
            return Err(BCSError::InvalidOffset);
        }
        Ok(Offset(value))
    }

    pub fn as_usize (&self) -> usize {
        return self.0;
    }

    pub fn serialize (&self, into: &mut [u8]) {
        use std::mem::transmute;

        let bytes: [u8; 8] = unsafe { transmute(self.0.to_be()) };
        into.copy_from_slice(&bytes[2 .. 8]);
    }
}
