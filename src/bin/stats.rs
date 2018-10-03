extern crate blockchain_store;
extern crate rand;
extern crate simple_logger;
extern crate log;

use blockchain_store::infile::InFile;
use blockchain_store::bcdb::BCDBFactory;
use blockchain_store::bcdb::BCDBAPI;

use blockchain_store::datafile::Content;

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
    let mut lllmin = <usize>::max_value();
    let mut lllmax = 0;
    let mut llln = 0;
    for (keys, _) in db.data_iterator() {
        if let Some(keys) = keys {
            data += 1;
        }
        else {
            ext += 1;
        }
    }
    for (v, mut next) in db.link_iterator() {
        loop {
            link += 1;
            ll += v.len();
            llmax = max(llmax, v.len());
            llmin = min(llmin, v.len());
            let mut lll = 1;
            loop {
                if !next.is_valid() {
                    break;
                }
                if let Ok((v, n)) = db.get_link(next) {
                    next = n;
                    lll += 1;
                    llln += 1;
                } else {
                    panic!("broken link chain");
                }
            }
            lllmax = max(lllmax, lll);
            lllmin = min(lllmin, lll);
        }
    }
    println!("data {} link {} {}/{}/{}/{}/{}/{} ext {}", data, link, llmin, llmax, ll/link, lllmin, lllmax, llln/link, ext);

    db.shutdown();
}