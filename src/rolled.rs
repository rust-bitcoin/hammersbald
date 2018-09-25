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
use bcdb::PageFile;
use error::BCSError;
use types::Offset;
use page::{Page, PAGE_SIZE};

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::path::Path;
use std::sync::Mutex;
use std::io::{Read,Write,Seek,SeekFrom};
use std::cmp::max;

const CHUNK_SIZE: u64 = 128*1024*1024;

pub struct RolledFile {
    name: String,
    extension: String,
    files: HashMap<u16, SingleFile>,
    len: u64,
    append_only: bool
}

impl RolledFile {
    pub fn new (name: String, extension: String, append_only: bool) -> Result<RolledFile, BCSError> {
        let mut rolled = RolledFile { name, extension, files: HashMap::new(), len: 0, append_only };
        rolled.open()?;
        Ok(rolled)
    }

    fn open (&mut self) -> Result<(), BCSError> {

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
                                                self.files.insert(number, SingleFile::new(file, number as u64 * CHUNK_SIZE)?);
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
            return Err(BCSError::Corrupted("invalid db name".to_string()));
        }
        Ok(())
    }

    fn open_file (append: bool, path: String) -> Result<File, BCSError> {
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

impl PageFile for RolledFile {
    fn flush(&mut self) -> Result<(), BCSError> {
        if self.append_only {
            let lc = (self.len / CHUNK_SIZE) as u16;
            if let Some (file) = self.files.get_mut(&lc) {
                file.flush()?;
            }
        }
        else {
            for file in &mut self.files.values_mut() {
                file.flush()?;
            }
        }
        Ok(())
    }

    fn len(&self) -> Result<u64, BCSError> {
        Ok(self.len)
    }

    fn truncate(&mut self, new_len: u64) -> Result<(), BCSError> {
        let chunk = (new_len / CHUNK_SIZE) as u16;
        for (c, file) in &mut self.files {
            if *c > chunk {
                file.truncate(0)?;
            }
        }
        if let Some (last) = self.files.get_mut(&chunk) {
            last.truncate(new_len % CHUNK_SIZE)?;
        }
        self.len = new_len;
        Ok(())
    }

    fn sync(&self) -> Result<(), BCSError> {
        if self.append_only {
            let lc = (self.len / CHUNK_SIZE) as u16;
            if let Some (file) = self.files.get(&lc) {
                file.sync()?;
            }
        }
        else {
            for file in self.files.values() {
                file.sync()?;
            }
        }
        Ok(())
    }

    fn read_page(&self, offset: Offset) -> Result<Page, BCSError> {
        let chunk = (offset.as_u64() / CHUNK_SIZE) as u16;
        if let Some(file) = self.files.get(&chunk) {
            return file.read_page(offset);
        }
        Err(BCSError::Corrupted("missing chunk in read".to_string()))
    }

    fn append_page(&mut self, page: Page) -> Result<(), BCSError> {
        let chunk = (self.len / CHUNK_SIZE) as u16;

        if self.len % CHUNK_SIZE == 0 && !self.files.contains_key(&chunk) {
            let file = Self::open_file(self.append_only, (((self.name.clone() + ".")
                + chunk.to_string().as_str()) + ".") + self.extension.as_str())?;
            self.files.insert(chunk, SingleFile::new(file, self.len)?);
            if chunk > 0 {
                if let Some(prev) = self.files.get_mut(&(chunk - 1)) {
                    prev.flush()?;
                    prev.sync()?;
                }
            }
        }

        if let Some (file) = self.files.get_mut(&chunk) {
            self.len += PAGE_SIZE as u64;
            return file.append_page(page);
        }
        else {
            return Err(BCSError::Corrupted("missing chunk in append".to_string()));
        }
    }

    fn write_page(&mut self, page: Page) -> Result<(), BCSError> {
        let offset = page.offset.as_u64();
        let chunk = (offset / CHUNK_SIZE) as u16;

        if self.len % CHUNK_SIZE == 0 && !self.files.contains_key(&chunk) {
            let file = Self::open_file(self.append_only, (((self.name.clone() + ".")
                + chunk.to_string().as_str()) + ".") + self.extension.as_str())?;
            self.files.insert(chunk, SingleFile::new(file, offset)?);
        }

        if let Some(file) = self.files.get_mut(&chunk) {
            self.len = max(self.len, offset + PAGE_SIZE as u64);
            return file.write_page(page);
        } else {
            return Err(BCSError::Corrupted("missing chunk in append".to_string()));
        }
    }
}

struct SingleFile {
    file: Mutex<File>,
    base: u64,
    len: u64
}

impl SingleFile {
    pub fn new (mut file: File, base: u64) -> Result<SingleFile, BCSError> {
        let len = file.seek(SeekFrom::End(0))?;
        Ok(SingleFile{file: Mutex::new(file), base, len})
    }
}

impl PageFile for SingleFile {
    fn flush(&mut self) -> Result<(), BCSError> {
        Ok(self.file.lock().unwrap().flush()?)
    }

    fn len(&self) -> Result<u64, BCSError> {
        Ok(self.len)
    }

    fn truncate(&mut self, new_len: u64) -> Result<(), BCSError> {
        self.len = new_len;
        Ok(self.file.lock().unwrap().set_len(new_len)?)
    }

    fn sync(&self) -> Result<(), BCSError> {
        Ok(self.file.lock().unwrap().sync_data()?)
    }

    fn read_page(&self, offset: Offset) -> Result<Page, BCSError> {
        let o = offset.as_u64();
        if o < self.base || o >= self.base + CHUNK_SIZE {
            return Err(BCSError::Corrupted("read from wrong file".to_string()));
        }
        let pos = o - self.base;
        if pos >= self.len {
            return Err(BCSError::InvalidOffset);
        }

        let mut file = self.file.lock().unwrap();
        let mut buffer = [0u8; PAGE_SIZE];
        file.seek(SeekFrom::Start(pos))?;
        file.read(&mut buffer)?;
        let page = Page::from_buf(buffer);
        Ok(page)
    }

    fn append_page(&mut self, page: Page) -> Result<(), BCSError> {
        let mut file = self.file.lock().unwrap();
        file.write(&page.finish()[..])?;
        self.len += PAGE_SIZE as u64;
        Ok(())
    }

    fn write_page(&mut self, page: Page) -> Result<(), BCSError> {
        let o = page.offset.as_u64();
        if o < self.base || o >= self.base + CHUNK_SIZE {
            return Err(BCSError::Corrupted("read from wrong file".to_string()));
        }
        let pos = o - self.base;

        let mut file = self.file.lock().unwrap();
        file.seek(SeekFrom::Start(pos))?;
        file.write(&page.finish()[..])?;
        self.len = max(self.len, pos + PAGE_SIZE as u64);
        Ok(())
    }
}