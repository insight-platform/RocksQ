use criterion::{criterion_group, criterion_main, Criterion};
use queue_rs::blocking::PersistentQueueWithCapacity;
use rocksdb::Options;

const COUNT: usize = 100;
const BLOCK_SIZE: usize = 64 * 1024;

fn rw_mixed(c: &mut Criterion) {
    let block = vec![0u8; BLOCK_SIZE];
    let path = "/tmp/test_b1".to_string();
    _ = PersistentQueueWithCapacity::remove_db(&path);

    c.bench_function("sync_rw_mixed", |b| {
        let db = PersistentQueueWithCapacity::new(&path, COUNT, Options::default()).unwrap();
        b.iter(|| {
            for _ in 0..COUNT {
                db.push(&[&block]).unwrap();
                db.pop(1).unwrap();
            }
        });
    });

    PersistentQueueWithCapacity::remove_db(&path).unwrap();
}

fn write_read(c: &mut Criterion) {
    let block = vec![0u8; BLOCK_SIZE];
    let path = "/tmp/test_b2".to_string();
    _ = PersistentQueueWithCapacity::remove_db(&path);

    c.bench_function("sync_write_read", |b| {
        let db = PersistentQueueWithCapacity::new(&path, COUNT, Options::default()).unwrap();
        b.iter(|| {
            for _ in 0..COUNT {
                db.push(&[&block]).unwrap();
            }
            for _ in 0..COUNT {
                db.pop(1).unwrap();
            }
        });
    });

    PersistentQueueWithCapacity::remove_db(&path).unwrap();
}

criterion_group!(benches, rw_mixed, write_read);
criterion_main!(benches);
