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
    let mut llmin = <usize>::max_value();
    let mut lllen = 0;
    for content in db.link_iterator() {
        match content {
            Content::Data(_, _) => {panic!("data in link file")},
            Content::Extension(_) => {panic!("extension in link file")},
            Content::Spillover(v, mut next) => {
                link += 1;
                ll += v.len();
                llmax=max(llmax, v.len());
                llmin=min(llmin, v.len());
                let mut lll = 1;
                loop {
                    if !next.is_valid() {
                        break;
                    }
                    if let Ok(Some((v, n))) = db.get_link(next) {
                        next = n;
                        lll += 1;
                    }
                    else {
                        panic!("broken link chain");
                    }
                }
                lllen = max(lllen, lll);
            },
        }
    }
    for content in db.data_iterator() {
        match content {
            Content::Data(_, _) => {data += 1; },
            Content::Extension(_) => {ext += 1; },
            Content::Spillover(v, next) => {panic!("spillover in data file")},
        }
    }
    println!("data {} link {} {}/{}/{}/{} ext {}", data, link, llmin, llmax, ll/link, lllen, ext);

    db.shutdown();
}