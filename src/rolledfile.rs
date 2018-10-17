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
//! # Rolled file
//!
//! A file that is split into chunks
//!
use error::BCDBError;
use offset::Offset;
use page::{Page, PAGE_SIZE};
use pagedfile::{FileOps, PagedFile, RandomWritePagedFile};
use singlefile::SingleFile;

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::path::Path;

pub struct RolledFile {
    name: String,
    extension: String,
    files: HashMap<u16,SingleFile>,
    len: u64,
    append_only: bool,
    chunk_size: u64
}

impl RolledFile {
    pub fn new (name: &str, extension: &str, append_only: bool, chunk_size: u64) -> Result<RolledFile, BCDBError> {
        let mut rolled = RolledFile { name: name.to_string(), extension: extension.to_string(), files: HashMap::new(), len: 0, append_only, chunk_size};
        rolled.open()?;
        Ok(rolled)
    }

    fn open (&mut self) -> Result<(), BCDBError> {

        // interesting file names are:
        // name.index.extension
        // where index is a number
        if let Some(mut dir) = Path::new(&self.name).parent() {
            if dir.to_string_lossy().to_string().is_empty() {
                dir = Path::new(".");
            }
            for entry in fs::read_dir(dir)? {
                let path = entry?.path();
                if path.is_file() {
                    if let Some (name_index) = path.file_stem() {
                        // name.index
                        let ni = Path::new(name_index.clone());
                        if let Some(name) = ni.file_stem() {
                            // compare name
                            if name.to_string_lossy().to_string() == self.name {
                                // compare extension
                                if let Some(extension) = path.extension() {
                                    if extension.to_string_lossy().to_string() == self.extension {
                                        // parse index
                                        if let Some(index) = ni.extension() {
                                            if let Ok(number) =  index.to_string_lossy().parse::<u16>() {
                                                let filename = path.clone().to_string_lossy().to_string();
                                                let file = Self::open_file(self.append_only, filename)?;
                                                self.files.insert(number,
                                                                  SingleFile::new_chunk(file, number as u64 * self.chunk_size, self.chunk_size)?);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        else {
            return Err(BCDBError::Corrupted("invalid db name".to_string()));
        }
        Ok(())
    }

    fn open_file (append: bool, path: String) -> Result<File, BCDBError> {
        let mut open_mode = OpenOptions::new();

        if append {
            open_mode.read(true).append(true).create(true);
        }
        else{
            open_mode.read(true).write(true).create(true);
        };
        Ok(open_mode.open(path)?)
    }
}

impl FileOps for RolledFile {
    fn flush(&mut self) -> Result<(), BCDBError> {
        for file in &mut self.files.values_mut() {
            file.flush()?;
        }
        Ok(())
    }

    fn len(&self) -> Result<u64, BCDBError> {
        Ok(self.len)
    }

    fn truncate(&mut self, new_len: u64) -> Result<(), BCDBError> {
        if new_len % PAGE_SIZE as u64 != 0 {
            return Err(BCDBError::Corrupted(format!("truncate not to page boundary {}", new_len)));
        }
        let chunk = (new_len / self.chunk_size) as u16;
        for (c, file) in &mut self.files {
            if *c > chunk {
                file.truncate(0)?;
            }
        }
        if let Some (last) = self.files.get_mut(&chunk) {
            last.truncate(new_len % self.chunk_size)?;
        }
        self.len = new_len;
        Ok(())
    }

    fn sync(&self) -> Result<(), BCDBError> {
        for file in self.files.values() {
            file.sync()?;
        }
        Ok(())
    }

    fn shutdown (&mut self) {}
}

impl PagedFile for RolledFile {
    fn read_page(&self, offset: Offset) -> Result<Option<Page>, BCDBError> {
        let chunk = (offset.as_u64() / self.chunk_size) as u16;
        if let Some(file) = self.files.get(&chunk) {
            return file.read_page(offset);
        }
        Ok(None)
    }

    fn append_page(&mut self, page: Page) -> Result<(), BCDBError> {
        let chunk = (self.len / self.chunk_size) as u16;

        if self.len % self.chunk_size == 0 && !self.files.contains_key(&chunk) {
            let file = Self::open_file(self.append_only, (((self.name.clone() + ".")
                + chunk.to_string().as_str()) + ".") + self.extension.as_str())?;
            self.files.insert(chunk, SingleFile::new_chunk(file, self.len, self.chunk_size)?);
        }

        if let Some (file) = self.files.get_mut(&chunk) {
            file.append_page(page)?;
            self.len += PAGE_SIZE as u64;
            return Ok(())
        }
        else {
            return Err(BCDBError::Corrupted(format!("missing chunk in append {}", chunk)));
        }
    }
}

impl RandomWritePagedFile for RolledFile {

    fn write_page(&mut self, offset: Offset, page: Page) -> Result<u64, BCDBError> {
        let n_offset = offset.as_u64();
        let chunk = (n_offset / self.chunk_size) as u16;

        if !self.files.contains_key(&chunk) {
            let file = Self::open_file(self.append_only, (((self.name.clone() + ".")
                + chunk.to_string().as_str()) + ".") + self.extension.as_str())?;
            self.files.insert(chunk, SingleFile::new_chunk(file, (n_offset/self.chunk_size) * self.chunk_size, self.chunk_size)?);
        }

        if let Some(file) = self.files.get_mut(&chunk) {
            self.len = file.write_page(offset, page)?  + chunk as u64 * self.chunk_size;
            Ok(self.len)
        } else {
            return Err(BCDBError::Corrupted(format!("missing chunk in write {}", chunk)));
        }
    }
}