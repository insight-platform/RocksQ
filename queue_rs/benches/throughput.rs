#![feature(test)]

extern crate test;

use queue_rs::PersistentQueueWithCapacity;
use rocksdb::Options;
use test::Bencher;

const COUNT: usize = 1000;

#[bench]
fn rw_mixed(b: &mut Bencher) {
    let block = vec![0u8; 256 * 1024];
    let path = "/tmp/test_b1".to_string();
    _ = PersistentQueueWithCapacity::remove_db(path.clone());
    {
        let mut db =
            PersistentQueueWithCapacity::new(path.clone(), COUNT as u64, Options::default())
                .unwrap();
        b.iter(|| {
            for _ in 0..COUNT {
                db.push(&block).unwrap();
                db.pop().unwrap();
            }
        });
    }
    PersistentQueueWithCapacity::remove_db(path).unwrap();
}

#[bench]
fn write_read(b: &mut Bencher) {
    let block = vec![0u8; 256 * 1024];
    let path = "/tmp/test_b2".to_string();
    _ = PersistentQueueWithCapacity::remove_db(path.clone());
    {
        let mut db =
            PersistentQueueWithCapacity::new(path.clone(), COUNT as u64, Options::default())
                .unwrap();
        b.iter(|| {
            for _ in 0..COUNT {
                db.push(&block).unwrap();
            }
            for _ in 0..COUNT {
                db.pop().unwrap();
            }
        });
    }
    PersistentQueueWithCapacity::remove_db(path).unwrap();
}
