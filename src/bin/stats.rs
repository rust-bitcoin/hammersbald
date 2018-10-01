extern crate blockchain_store;
extern crate rand;
extern crate simple_logger;
extern crate log;

use blockchain_store::infile::InFile;
use blockchain_store::bcdb::BCDBFactory;
use blockchain_store::bcdb::BCDBAPI;

use rand::{thread_rng, Rng};
use blockchain_store::datafile::Content;

use std::time::{Instant};

pub fn main () {
    simple_logger::init_with_level(log::Level::Info).unwrap();
    let mut db = InFile::new_db("testdb").unwrap();
    db.init().unwrap();

    let mut data = 0;
    for content in db.data_iterator() {
        match content {
            Content::Data(_, _) => data += 1,
            Content::Extension(_) => {println!("extension"); break},
            Content::Spillover(v, next) => {println!("spill {} {}", v.len(), next); break},
        }
    }
    println!("number of data entries {} ", data);

    db.shutdown();
}