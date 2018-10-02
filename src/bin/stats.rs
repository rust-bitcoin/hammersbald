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
use std::cmp::{max, min};

pub fn main () {
    simple_logger::init_with_level(log::Level::Info).unwrap();
    let mut db = InFile::new_db("testdb").unwrap();
    db.init().unwrap();

    let mut data = 0;
    let mut link = 0;
    let mut ext = 0;
    let mut ll = 0;
    let mut llmax = 0;
    let mut llmin = 0;
    for content in db.link_iterator() {
        match content {
            Content::Data(_, _) => {data += 1; },
            Content::Extension(_) => {ext += 1; },
            Content::Spillover(v, next) => {link += 1; ll += v.len(); llmax=max(llmax, ll); llmin=min(llmin, ll);},
        }
    }
    println!("data {} link {} {}/{}/{} ext {}", data, link, llmin, llmax, ll/link, ext);

    db.shutdown();
}