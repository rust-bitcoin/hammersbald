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
//! # Rolled file
//!
//! A file that is split into chunks
//!
use error::HammersbaldError;
use pref::PRef;
use page::{Page, PAGE_SIZE};
use pagedfile::PagedFile;
use singlefile::SingleFile;

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::path::Path;
use std::cmp::{max, min};

pub struct RolledFile {
    name: String,
    extension: String,
    files: HashMap<u16,SingleFile>,
    len: u64,
    append_only: bool,
    chunk_size: u64
}

impl RolledFile {
    pub fn new (name: &str, extension: &str, append_only: bool, chunk_size: u64) -> Result<RolledFile, HammersbaldError> {
        let mut rolled = RolledFile { name: name.to_string(), extension: extension.to_string(), files: HashMap::new(), len: 0, append_only, chunk_size};
        rolled.open()?;
        Ok(rolled)
    }

    fn open (&mut self) -> Result<(), HammersbaldError> {
        // interesting file names are:
        // name.index.extension
        // where index is a number
        if let Some(basename) = Path::new(self.name.as_str()).file_name() {
            let mut highest_chunk = 0;
            if let Some(mut dir) = Path::new(&self.name).parent() {
                if dir.to_string_lossy().to_string().is_empty() {
                    dir = Path::new(".");
                }
                for entry in fs::read_dir(dir)? {
                    let path = entry?.path();
                    if path.is_file() {
                        if let Some(name_index) = path.file_stem() {
                            // name.index
                            let ni = Path::new(name_index.clone());
                            if let Some(name) = ni.file_stem() {
                                // compare name
                                if name == basename {
                                    // compare extension
                                    if let Some(extension) = path.extension() {
                                        if extension.to_string_lossy().to_string() == self.extension {
                                            // parse index
                                            if let Some(index) = ni.extension() {
                                                if let Ok(number) = index.to_string_lossy().parse::<u16>() {
                                                    let filename = path.clone().to_string_lossy().to_string();
                                                    let file = Self::open_file(self.append_only, filename)?;
                                                    self.files.insert(number,
                                                                      SingleFile::new_chunk(file, number as u64 * self.chunk_size, self.chunk_size)?);
                                                    if let Some (file) = self.files.get(&number) {
                                                        if file.len().unwrap() > 0 {
                                                            highest_chunk = max(highest_chunk, number);
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
                }
            }
            if let Some (file) = self.files.get(&highest_chunk) {
                self.len = highest_chunk as u64 * self.chunk_size + file.len()?;
            }
        }
        else {
            return Err(HammersbaldError::Corrupted("invalid db name".to_string()));
        }
        Ok(())
    }

    fn open_file (append: bool, path: String) -> Result<File, HammersbaldError> {
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

impl PagedFile for RolledFile {
    fn read_page(&self, pref: PRef) -> Result<Option<Page>, HammersbaldError> {
        let result = self.read_pages(pref, 1)?;
        if let Some (page) = result.first() {
            Ok(Some(page.clone()))
        }
        else {
            Ok(None)
        }
    }

    fn read_pages(&self, mut pref: PRef, n: usize) -> Result<Vec<Page>, HammersbaldError> {
        let mut result = Vec::new();
        while result.len() < n {
            let chunk = (pref.as_u64() / self.chunk_size) as u16;
            if let Some(file) = self.files.get(&chunk) {
                let has = min(result.len() - n, ((self.chunk_size - pref.as_u64() % self.chunk_size) / PAGE_SIZE as u64) as usize);
                result.extend(file.read_pages(pref, has)?);
                pref = pref.add_pages(has);
            }
            else {
                break;
            }
        }
        Ok(result)
    }

    fn len(&self) -> Result<u64, HammersbaldError> {
        Ok(self.len)
    }

    fn truncate(&mut self, new_len: u64) -> Result<(), HammersbaldError> {
        if new_len % PAGE_SIZE as u64 != 0 {
            return Err(HammersbaldError::Corrupted(format!("truncate not to page boundary {}", new_len)));
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

    fn sync(&self) -> Result<(), HammersbaldError> {
        for file in self.files.values() {
            file.sync()?;
        }
        Ok(())
    }

    fn shutdown (&mut self) {}

    fn append_pages (&mut self, pages: &Vec<Page>) -> Result<(), HammersbaldError> {
        let mut start = 0;
        while start < pages.len() {
            let chunk = (self.len / self.chunk_size) as u16;

            if self.len % self.chunk_size == 0 && !self.files.contains_key(&chunk) {
                let file = Self::open_file(self.append_only, (((self.name.clone() + ".")
                    + chunk.to_string().as_str()) + ".") + self.extension.as_str())?;
                self.files.insert(chunk, SingleFile::new_chunk(file, self.len, self.chunk_size)?);
            }

            if let Some (file) = self.files.get_mut(&chunk) {
                let fits = (self.chunk_size - self.len % self.chunk_size) as usize/PAGE_SIZE;
                let write = min(fits, pages.len() - start);
                file.append_pages(&pages[start .. start + write].to_vec())?;
                start += write;
                self.len += (write*PAGE_SIZE) as u64;
            }
            else {
                return Err(HammersbaldError::Corrupted(format!("missing chunk in append {}", chunk)));
            }
        }
        Ok(())
    }

    fn update_page(&mut self, page: Page) -> Result<u64, HammersbaldError> {
        let n_offset = page.pref().as_u64();
        let chunk = (n_offset / self.chunk_size) as u16;

        if !self.files.contains_key(&chunk) {
            let file = Self::open_file(self.append_only, (((self.name.clone() + ".")
                + chunk.to_string().as_str()) + ".") + self.extension.as_str())?;
            self.files.insert(chunk, SingleFile::new_chunk(file, (n_offset/self.chunk_size) * self.chunk_size, self.chunk_size)?);
        }

        if let Some(file) = self.files.get_mut(&chunk) {
            self.len = file.update_page(page)?  + chunk as u64 * self.chunk_size;
            Ok(self.len)
        } else {
            return Err(HammersbaldError::Corrupted(format!("missing chunk in write {}", chunk)));
        }
    }

    fn flush(&mut self) -> Result<(), HammersbaldError> {
        for file in &mut self.files.values_mut() {
            file.flush()?;
        }
        Ok(())
    }
}