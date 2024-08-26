#![feature(test)]

extern crate test;

use queue_rs::blocking::PersistentQueueWithCapacity;
use rocksdb::Options;
use test::Bencher;

const COUNT: usize = 10;

#[bench]
fn rw_mixed(b: &mut Bencher) {
    let block = vec![0u8; 256 * 1024];
    let path = "/tmp/test_b1".to_string();
    _ = PersistentQueueWithCapacity::remove_db(&path);
    {
        let db = PersistentQueueWithCapacity::new(&path, COUNT, Options::default()).unwrap();
        b.iter(|| {
            for _ in 0..COUNT {
                db.push(&[&block]).unwrap();
                db.pop(1).unwrap();
            }
        });
    }
    PersistentQueueWithCapacity::remove_db(&path).unwrap();
}

#[bench]
fn write_read(b: &mut Bencher) {
    let block = vec![0u8; 256 * 1024];
    let path = "/tmp/test_b2".to_string();
    _ = PersistentQueueWithCapacity::remove_db(&path);
    {
        let db = PersistentQueueWithCapacity::new(&path, COUNT, Options::default()).unwrap();
        b.iter(|| {
            for _ in 0..COUNT {
                db.push(&[&block]).unwrap();
            }
            for _ in 0..COUNT {
                db.pop(1).unwrap();
            }
        });
    }
    PersistentQueueWithCapacity::remove_db(&path).unwrap();
}
