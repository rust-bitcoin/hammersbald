extern crate blockchain_store;
extern crate rand;
extern crate simple_logger;
extern crate log;

use blockchain_store::infile::InFile;
use blockchain_store::bcdb::BCDBFactory;
use blockchain_store::bcdb::BCDBAPI;

use blockchain_store::types::Offset;

use std::cmp::{max, min};

pub fn main () {
    simple_logger::init_with_level(log::Level::Info).unwrap();
    let mut db = InFile::new_db("testdb").unwrap();
    db.init().unwrap();


    let mut data = 0;
    let mut data_len = 0;
    let mut link = 0;
    let mut ext = 0;
    let mut ext_len = 0;
    let mut ll = 0;
    let mut llmax = 0;
    let mut llmin = <usize>::max_value();
    let mut lllmin = <usize>::max_value();
    let mut lllmax = 0;
    let mut llln = 0;
    for (keys, d) in db.data_iterator() {
        if let Some(keys) = keys {
            data += 1;
            data_len += d.len();
        }
        else {
            ext += 1;
            ext_len += d.len();
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
    data_len = max(data_len, 1);
    ext_len = max(ext_len, 1);
    link = max(link, 1);
    println!("data {}/{} link {} {}/{}/{}/{}/{}/{} ext {}/{}", data, data as f32 / data_len as f32, link, llmin, llmax,
             ll as f32/link as f32, lllmin, lllmax, llln as f32/link as f32, ext, ext as f32/ext_len as f32);

    let mut first_link_offset = Offset::invalid();
    let mut last_link_offset = Offset::from(0);
    let mut first_active_data = Offset::invalid();
    let mut last_active_data = Offset::from(0);    let mut used_buckets = 0;
    let mut active_data = 0;
    let mut active_links = 0;
    let mut shortest_link_vec = 0xffu8;
    let mut longest_link_vec = 0u8;
    for offset in db.iter() {
        let mut link_offset = offset;
        loop {
            if link_offset.is_valid() {
                used_buckets += 1;
                first_link_offset = min(first_link_offset, link_offset);
                last_link_offset = max(last_link_offset, link_offset);

                if let Ok((ref links, next)) = db.get_link(link_offset) {
                    active_data += links.len();
                    active_links += 1;
                    for data_offset in links {
                        first_active_data = min(first_active_data, *data_offset);
                        last_active_data = max(last_active_data, *data_offset);
                    }
                    shortest_link_vec = min(shortest_link_vec, links.len() as u8);
                    longest_link_vec = max(longest_link_vec, links.len() as u8);

                    link_offset = next;
                } else {
                    panic!("can not find root link {}", offset);
                }
            }
            else {
                break;
            }
        }
    }

    active_links = max(active_links, 1);
    println!("active data: {} {}-{} buckets: {} links: {}/{}/{} {}-{}", active_data, first_active_data, last_active_data,
             used_buckets,
             shortest_link_vec, longest_link_vec, active_data as f32/active_links as f32,
        first_link_offset, last_link_offset
            );

    db.shutdown();
}