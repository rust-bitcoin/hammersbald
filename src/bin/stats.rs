extern crate hammersbald;
extern crate rand;
extern crate simple_logger;
extern crate log;
extern crate siphasher;

use hammersbald::persistent::Persistent;
use hammersbald::api::HammersbaldFactory;
use hammersbald::api::HammersbaldAPI;

use hammersbald::format::Payload;

use log::Level;
use siphasher::sip::SipHasher;
use std::hash::Hasher;

use std::env::args;
use std::collections::{HashSet, HashMap};

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

    let (step, log_mod, blen, tlen, dlen, llen, sip0, sip1) = db.params();
    println!("File sizes: table: {}, data: {}, links: {}\nHash table: buckets: {}, log_mod: {}, step: {}", tlen, dlen, llen, blen, log_mod, step);

    let mut pointer = HashSet::new();
    for bucket in db.buckets() {
        if bucket.is_valid() {
            pointer.insert(bucket);
        }
    }

    let mut n_links = 0;
    for (pos, envelope) in db.link_envelopes() {
        match Payload::deserialize(envelope.payload()).unwrap() {
            Payload::Link(_) => {
                n_links += 1;
                pointer.remove (&pos);
            },
            _ => panic!("Unexpected payload type link at {}", pos)
        }
    }
    if !pointer.is_empty() {
        panic!("{} roots point to non-existent links", pointer.len());
    }


    let mut roots = HashMap::new();
    let mut ndata = 0;
    let mut used_buckets = 0;
    for slots in db.slots() {
        ndata += slots.len();
        if slots.len() > 0 {
            used_buckets += 1;
        }
        for slot in slots.iter() {
            roots.entry(slot.1).or_insert(Vec::new()).push(slot.0);
        }
    }
    println!("Used buckets: {} {} %", used_buckets, 100.0*(used_buckets as f32/blen as f32));
    println!("Data: indexed: {}, hash collisions {:.2} %", ndata, (1.0-(roots.len() as f32)/(ndata as f32))*100.0);

    let mut indexed_garbage = 0;
    let mut referred_garbage = 0;
    let mut referred = 0;
    let mut referred_set = HashSet::new();
    for (pos, envelope) in db.data_envelopes() {
        match Payload::deserialize(envelope.payload()).unwrap() {
            Payload::Indexed(indexed) => {
                if let Some(root) = roots.remove(&pos) {
                    let h = hash(indexed.key, sip0, sip1);
                    if root.iter().any(|hash| *hash == h) == false {
                        panic!("ERROR root {} points data with different key hash", pos);
                    }
                    indexed.data.referred().iter().for_each(|o| {referred_set.insert(*o);});
                } else {
                    indexed_garbage += 1;
                }
                referred_set.remove(&pos);
            },
            Payload::Referred(data) => {
                if !referred_set.remove(&pos) {
                    referred_garbage += 1;
                }
                referred += 1;
                data.referred().iter().for_each(|o| {referred_set.insert(*o);});
            },
            _ => panic!("Unexpected payload type in data at {}", pos)
        }
    }
    if !roots.is_empty() {
        panic!("ERROR {} roots point to non-existent data", roots.len());
    }
    if !referred_set.is_empty() {
        panic!("ERROR {} references point to nowhere", referred_set.len());
    }
    println!("Referred: {}", referred);
    println!("Garbage: indexed: {}, referred: {}, links: {}", indexed_garbage, referred_garbage, n_links - used_buckets);

    db.shutdown();
}

fn hash (key: &[u8], sip0: u64, sip1: u64) -> u32 {
    let mut hasher = SipHasher::new_with_keys(sip0, sip1);
    hasher.write(key);
    hasher.finish() as u32
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
