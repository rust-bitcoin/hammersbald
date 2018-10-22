extern crate bcdb;
extern crate rand;
extern crate simple_logger;
extern crate log;

use bcdb::persistent::Persistent;
use bcdb::api::BCDBFactory;
use bcdb::api::BCDBAPI;

use log::Level;

use std::env::args;
use std::collections::HashMap;

pub fn main () {
    if find_opt("help") {
        println!("{} [--help] [--stats data|links|accessible] [--db database] [--log trace|debug|info|warn|error] --cache pages", args().next().unwrap());
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
        println!("--cache 100");
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

    let mut cache = 100;
    if let Some(path) = find_arg("cache") {
        cache = path.parse::<usize>().unwrap();
    }

    let mut db;
    if let Some(path) = find_arg("db") {
        db = Persistent::new_db(path.as_str(), cache).unwrap();
    } else {
        db = Persistent::new_db("testdb", cache).unwrap();
    }

    db.init().unwrap();

    let (step, log_mod, blen, tlen, dlen) = db.params();
    println!("table {} data {} buckets {} log_mod {} step {}", tlen, dlen, blen, log_mod, step);
    let mut roots = HashMap::new();
    for bucket in db.buckets() {
        for slot in bucket.iter() {
            if slot.1.is_valid() {
                roots.entry(slot.0).or_insert(Vec::new()).push(slot.1);
            }
        }
    }
    println!("roots {}", roots.len());


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

#[allow(unused)]
fn find_args(key: &str) -> Vec<String> {
    zipped_args().filter(|&(ref k, _)| k.as_str() == key).map(|(_, v)| v).collect()
}
