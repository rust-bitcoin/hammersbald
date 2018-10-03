extern crate blockchain_store;
extern crate rand;
extern crate simple_logger;
extern crate log;

use blockchain_store::infile::InFile;
use blockchain_store::bcdb::BCDBFactory;
use blockchain_store::bcdb::BCDBAPI;

use rand::{thread_rng, Rng};

use std::time::{Instant};

pub fn main () {
    simple_logger::init_with_level(log::Level::Info).unwrap();
    let mut db = InFile::new_db("testdb").unwrap();
    db.init().unwrap();

    // transaction size assumed 500 bytes
    let mut data = [0x0u8;500];

    // simulating a blockchain ingest

    // number of transactions
    let ntx = 20000000;
    // transactions per block
    let tb = 1000;
    // load batch size (in number of blocks)
    let bat = 1000;

    // check keys
    let mut check = Vec::with_capacity((ntx as usize)/100);


    println!("Inserting data ...");
    let mut n = 0;
    let mut now = Instant::now();
    let mut elapsed;
    let mut key = [0u8;32];
    for i in 0 .. ntx {
        thread_rng().fill(&mut data[..]);
        thread_rng().fill(&mut key[..]);
        if i % 100 == 0 {
            check.push ((key.clone(), data.clone()));
        }
        db.put(vec!(key.to_vec()), &data).unwrap();
        n += 1;

        if n % (bat*tb) == 0 {
            db.batch().unwrap();
            elapsed = now.elapsed().as_secs();
            println!("Stored {} million transactions in {} seconds, {} inserts/second.", n/1000000, elapsed, n/elapsed);
        }
    }

    db.batch().unwrap();
    elapsed = now.elapsed().as_secs();
    println!("Stored {} million transactions in {} seconds, {} inserts/second ", ntx/1000000, elapsed, ntx/elapsed);

    println!("Shuffle keys...");
    thread_rng().shuffle(&mut check);
    println!("Reading data in random order...");
    now = Instant::now();
    for (key, data) in &check {
        assert_eq!(db.get_unique(key).unwrap(), Some(data.to_vec()));
    }
    elapsed = now.elapsed().as_secs();
    if elapsed > 0 {
        println!("Read {} million transactions in {} seconds, {} read/second ", (ntx/100) / 1000000, elapsed, (ntx/100) / elapsed);
    }

    db.shutdown();
}