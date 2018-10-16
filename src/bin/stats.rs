extern crate bcdb;
extern crate rand;
extern crate simple_logger;
extern crate log;

use bcdb::persistent::Persistent;
use bcdb::api::BCDBFactory;
use bcdb::api::BCDBAPI;

use bcdb::offset::Offset;

use log::Level;

use std::cmp::{max, min};
use std::env::args;

pub fn main () {
    if find_opt("help") {
        println!("{} [--help] [--stats data|links|accessible] [--db database] [--log trace|debug|info|warn|error]", args().next().unwrap());
        println!("--stats what:");
        println!("        accessible: accessible stored data and links");
        println!("        data: all stored data even if no longer accessible");
        println!("        links: all stored links even if no longer accessible");
        println!("--db name: store base name. Created if does not exist.");
        println!("--log level: level is one of trace|debug|info|warn|error");
        println!("defaults:");
        println!("--log info");
        println!("--stats accessible");
        println!("--db testdb");
        return;
    }

    if let Some (log) = find_arg("log") {
        match log.as_str() {
            "error" => simple_logger::init_with_level(Level::Error).unwrap(),
            "warn" => simple_logger::init_with_level(Level::Warn).unwrap(),
            "info" => simple_logger::init_with_level(Level::Info).unwrap(),
            "debug" => simple_logger::init_with_level(Level::Debug).unwrap(),
            "trace" => simple_logger::init_with_level(Level::Trace).unwrap(),
            _ => panic!("unknown log level")
        }
    } else {
            simple_logger::init_with_level(Level::Info).unwrap();
    }

    let mut db;
    if let Some(path) = find_arg("db") {
        db = Persistent::new_db(path.as_str()).unwrap();
    } else {
        db = Persistent::new_db("testdb").unwrap();
    }

    db.init().unwrap();

    let mut stats = find_args("stats");
    if stats.is_empty() {
        stats.push ("accessible".to_string());
    }

    for what in stats {
        if what == "data" {
            let mut data = 0;
            let mut data_len = 0;
            let mut ext = 0;
            let mut ext_len = 0;

            for (_, _, d) in db.data_iterator() {
                data += 1;
                data_len += d.len();
                // TODO: fix extensions
            }
            println!("stored key accessible data elements: {}, total {} bytes", data, data_len);
            println!("stored data extensions: {}, total {} bytes", ext, ext_len);
        }


        if what == "links" {
            let mut link = 0;
            let mut lvnmax = 0;
            let mut lvmin = <usize>::max_value();
            let mut lvlen = 0;
            let mut first_link_offset = Offset::invalid();
            let mut last_link_offset = Offset::from(0);
            for (_, v, _) in db.link_iterator() {
                link += 1;
                lvlen += v.len();
                lvnmax = max(lvnmax, v.len());
                lvmin = min(lvmin, v.len());
                for link_offset in v {
                    first_link_offset = min(first_link_offset, link_offset.1);
                    last_link_offset = max(last_link_offset, link_offset.1);
                }
            }
            print!("stored links {} ", link);
            link = max(link, 1);
            println!("link vector lengts: min/max {}/{} average: {} store range: {}-{}",
                     lvmin, lvnmax, lvlen as f32 / link as f32, first_link_offset, last_link_offset);
        }

        if what == "accessible" {
            let mut first_link_offset = Offset::invalid();
            let mut last_link_offset = Offset::from(0);
            let mut first_active_data = Offset::invalid();
            let mut last_active_data = Offset::from(0);
            let mut used_buckets = 0;
            let mut active_links = 0;
            let mut shallow_chain = <u32>::max_value();
            let mut deep_chain = 0;
            let mut active_links_len = 0;
            let mut shortest_link_vec = 0xffu8;
            let mut longest_link_vec = 0u8;
            let mut data = 0;
            let mut data_len = 0;
            let mut n_keys = 0;
            let mut key_len = 0;
            let mut ext = 0;
            let mut ext_len = 0;
            for offset in db.bucket_iterator() {
                let mut link_offset = offset;
                let mut current_depth = 0;
                if link_offset.is_valid() {
                    used_buckets += 1;
                }
                loop {
                    if link_offset.is_valid() {
                        active_links += 1;
                        current_depth += 1;
                        first_link_offset = min(first_link_offset, link_offset);
                        last_link_offset = max(last_link_offset, link_offset);

                        if let Ok((ref links, next)) = db.get_link(link_offset) {
                            active_links_len += links.len();
                            shortest_link_vec = min(shortest_link_vec, links.len() as u8);
                            longest_link_vec = max(longest_link_vec, links.len() as u8);

                            /* TODO
                            for (_, data_offset) in links {
                                first_active_data = min(first_active_data, *data_offset);
                                last_active_data = max(last_active_data, *data_offset);
                                let d = db.get_content(*data_offset).unwrap();
                                data += 1;
                                data_len += d.len();
                            }
                            */
                            link_offset = next;

                        } else {
                            panic!("can not find root link {}", offset);
                        }
                    } else {
                        break;
                    }
                }
                shallow_chain = min(shallow_chain, current_depth);
                deep_chain = max(deep_chain, current_depth);
            }

            print!("accessible data: {} ", data);
            data = max(data, 1);
            println!("avg. length: {} store range: {}-{}",
                     data_len as f32 / data as f32, first_active_data, last_active_data);

            print!("accessible keys: {} ", n_keys);
            n_keys = max(n_keys, 1);
            println!("avg. length: {}", key_len as f32 / n_keys as f32);

            print!("accessible data extensions: {} ", ext);
            ext = max(ext, 1);
            println!("avg. length: {}",
                     ext_len as f32 / ext as f32);

            print!("used buckets: {} ", used_buckets);

            used_buckets = max(used_buckets, 1);
            println!("avg. holding: {}",
                     data as f32 / used_buckets as f32);

            println!("accessible links: {} ", active_links);
            println!("link chain depth min/max/average: {}/{}/{}", shallow_chain, deep_chain, active_links as f32 / used_buckets as f32);
            active_links = max(active_links, 1);
            println!("link vector lengts: min/max {}/{} average: {} store range: {}-{}",
                     shortest_link_vec, longest_link_vec, active_links_len as f32 / active_links as f32,
                     first_link_offset, last_link_offset);
            println!("check: buckets * avg. link chain depth * avg. vector lengths ({}) ~= accessible data ({})",
                     used_buckets as f32 * active_links as f32 / used_buckets as f32 *
                         active_links_len as f32 / active_links as f32, data);
        }
    }
    db.shutdown();
}

// Returns key-value zipped iterator.
fn zipped_args() -> impl Iterator<Item = (String, String)> {
    let key_args = args().filter(|arg| arg.starts_with("--")).map(|mut arg| arg.split_off(2));
    let val_args = args().skip(1).filter(|arg| !arg.starts_with("--"));
    key_args.zip(val_args)
}

fn find_opt(key: &str) -> bool {
    let mut key_args = args().filter(|arg| arg.starts_with("--")).map(|mut arg| arg.split_off(2));
    key_args.find(|ref k| k.as_str() == key).is_some()
}

fn find_arg(key: &str) -> Option<String> {
    zipped_args().find(|&(ref k, _)| k.as_str() == key).map(|(_, v)| v)
}

fn find_args(key: &str) -> Vec<String> {
    zipped_args().filter(|&(ref k, _)| k.as_str() == key).map(|(_, v)| v).collect()
}
