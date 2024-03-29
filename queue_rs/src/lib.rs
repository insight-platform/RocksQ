pub mod blocking;
mod fs;
pub mod nonblocking;

use anyhow::Result;
use rocksdb::{Options, DB};

pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[derive(Debug)]
pub struct PersistentQueueWithCapacity {
    db: DB,
    path: String,
    space_stat: u64,
    write_index: u64,
    read_index: u64,
    max_elements: u64,
}

const U64_BYTE_LEN: usize = 8;
const WRITE_INDEX_CELL: u64 = u64::MAX;
const READ_INDEX_CELL: u64 = u64::MAX - 1;
const SPACE_STAT_CELL: u64 = u64::MAX - 2;

#[cfg(test)]
const MAX_ALLOWED_INDEX: u64 = 4;
#[cfg(not(test))]
const MAX_ALLOWED_INDEX: u64 = u64::MAX - 100;

// db_opts.set_write_buffer_size(64 * 1024 * 1024);
// db_opts.set_max_write_buffer_number(5);
// db_opts.set_min_write_buffer_number_to_merge(2);

impl PersistentQueueWithCapacity {
    pub fn new(path: &str, max_elements: usize, mut db_opts: Options) -> Result<Self> {
        db_opts.create_if_missing(true);
        db_opts.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(U64_BYTE_LEN));

        let db = DB::open(&db_opts, path)?;

        let write_index_opt = db.get(Self::index_to_key(WRITE_INDEX_CELL))?;
        let write_index = match write_index_opt {
            Some(v) => {
                let mut buf = [0u8; U64_BYTE_LEN];
                buf.copy_from_slice(&v);
                u64::from_le_bytes(buf)
            }
            None => 0u64,
        };

        let read_index_opt = db.get(Self::index_to_key(READ_INDEX_CELL))?;
        let read_index = match read_index_opt {
            Some(v) => {
                let mut buf = [0u8; U64_BYTE_LEN];
                buf.copy_from_slice(&v);
                u64::from_le_bytes(buf)
            }
            None => 0u64,
        };

        let space_stat_opt = db.get(Self::index_to_key(SPACE_STAT_CELL))?;
        let space_stat = match space_stat_opt {
            Some(v) => {
                let mut buf = [0u8; U64_BYTE_LEN];
                buf.copy_from_slice(&v);
                u64::from_le_bytes(buf)
            }
            None => 0u64,
        };

        Ok(Self {
            db,
            path: path.to_string(),
            write_index,
            read_index,
            space_stat,
            max_elements: max_elements as u64,
        })
    }

    fn index_to_key(index: u64) -> [u8; U64_BYTE_LEN] {
        index.to_le_bytes()
    }

    pub fn remove_db(path: &str) -> Result<()> {
        Ok(DB::destroy(&Options::default(), path)?)
    }

    pub fn disk_size(&self) -> Result<usize> {
        Ok(fs::dir_size(&self.path)?)
    }

    pub fn len(&self) -> usize {
        (if self.write_index >= self.read_index {
            self.write_index - self.read_index
        } else {
            MAX_ALLOWED_INDEX - self.read_index + self.write_index
        }) as usize
    }

    pub fn payload_size(&self) -> u64 {
        self.space_stat
    }

    pub fn is_empty(&self) -> bool {
        self.write_index == self.read_index
    }

    pub fn push(&mut self, values: &[&[u8]]) -> Result<()> {
        if self.len() + values.len() > self.max_elements as usize {
            return Err(anyhow::anyhow!("Queue is full"));
        }

        let mut batch = rocksdb::WriteBatch::default();

        for value in values {
            batch.put(Self::index_to_key(self.write_index), value);
            self.write_index += 1;
            if self.write_index == MAX_ALLOWED_INDEX {
                self.write_index = 0;
            }
        }

        batch.put(
            Self::index_to_key(WRITE_INDEX_CELL),
            self.write_index.to_le_bytes(),
        );

        self.space_stat += values.iter().map(|v| v.len() as u64).sum::<u64>();

        batch.put(
            Self::index_to_key(SPACE_STAT_CELL),
            self.space_stat.to_le_bytes(),
        );

        self.db.write(batch)?;

        Ok(())
    }

    pub fn pop(&mut self, mut max_elts: usize) -> Result<Vec<Vec<u8>>> {
        let mut res = Vec::with_capacity(max_elts);
        let mut batch = rocksdb::WriteBatch::default();
        loop {
            let key = Self::index_to_key(self.read_index);
            let value = self.db.get(key)?;
            if let Some(v) = value {
                batch.delete(key);
                res.push(v);
                self.read_index += 1;
                if self.read_index == MAX_ALLOWED_INDEX {
                    self.read_index = 0;
                }
                max_elts -= 1;
            } else {
                break;
            }

            if self.read_index != self.write_index && max_elts > 0 {
                continue;
            } else {
                break;
            }
        }
        if !res.is_empty() {
            self.space_stat -= res.iter().map(|v| v.len() as u64).sum::<u64>();
            batch.put(
                Self::index_to_key(SPACE_STAT_CELL),
                self.space_stat.to_le_bytes(),
            );
            batch.put(
                Self::index_to_key(READ_INDEX_CELL),
                self.read_index.to_le_bytes(),
            );
            self.db.write(batch)?;
        }

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_normal_ops() {
        let path = "/tmp/test1".to_string();
        _ = PersistentQueueWithCapacity::remove_db(&path);
        {
            let mut db = PersistentQueueWithCapacity::new(&path, 10, Options::default()).unwrap();
            db.push(&[&[1, 2, 3]]).unwrap();
            db.push(&[&[4, 5, 6]]).unwrap();
            assert_eq!(db.len(), 2);
            assert_eq!(db.payload_size(), 6);
            assert!(matches!(db.pop(1), Ok(v ) if v == vec![vec![1, 2, 3]]));
            assert!(matches!(db.pop(1), Ok(v) if v == vec![vec![4, 5, 6]]));
            assert!(db.is_empty());
            db.push(&[&[1, 2, 3]]).unwrap();
            db.push(&[&[4, 5, 6]]).unwrap();
            db.push(&[&[7, 8, 9]]).unwrap();
            assert_eq!(db.len(), 3);
            assert_eq!(db.read_index, 2);
            assert_eq!(db.write_index, 1);
            assert!(matches!(db.pop(1), Ok(v) if v == vec![vec![1, 2, 3]]));
            assert_eq!(db.len(), 2);
            assert_eq!(db.read_index, 3);
            assert_eq!(db.write_index, 1);
            assert!(matches!(db.pop(1), Ok(v) if v == vec![vec![4, 5, 6]]));
            assert_eq!(db.len(), 1);
            assert_eq!(db.read_index, 0);
            assert_eq!(db.write_index, 1);
            let data = db.pop(1).unwrap();
            assert!(db.is_empty());
            assert_eq!(data, vec![vec![7, 8, 9]]);
        }
        PersistentQueueWithCapacity::remove_db(&path).unwrap();
    }

    #[test]
    fn test_limit_len_ops() {
        let path = "/tmp/test2".to_string();
        _ = PersistentQueueWithCapacity::remove_db(&path);
        {
            let mut db = PersistentQueueWithCapacity::new(&path, 2, Options::default()).unwrap();
            db.push(&[&[1, 2, 3]]).unwrap();
            db.push(&[&[4, 5, 6]]).unwrap();
            assert!(matches!(db.push(&[&[1, 2, 3]]), Err(_)));
            assert_eq!(db.payload_size(), 6);
        }
        PersistentQueueWithCapacity::remove_db(&path).unwrap();
    }

    #[test]
    fn test_read_with_close() {
        let path = "/tmp/test3".to_string();
        _ = PersistentQueueWithCapacity::remove_db(&path);
        {
            let mut db = PersistentQueueWithCapacity::new(&path, 10, Options::default()).unwrap();
            db.push(&[&[1, 2, 3]]).unwrap();
            db.push(&[&[4, 5, 6]]).unwrap();
            db.push(&[&[7, 8, 9]]).unwrap();
        }

        {
            let mut db = PersistentQueueWithCapacity::new(&path, 10, Options::default()).unwrap();
            assert_eq!(db.payload_size(), 9);
            let res = db.pop(1).unwrap();
            assert_eq!(res, vec![vec![1, 2, 3]]);
            assert_eq!(db.payload_size(), 6);
        }

        {
            let mut db = PersistentQueueWithCapacity::new(&path, 10, Options::default()).unwrap();
            let res = db.pop(1).unwrap();
            assert_eq!(res, vec![vec![4, 5, 6]]);
            let res = db.pop(1).unwrap();
            assert_eq!(res, vec![vec![7, 8, 9]]);
        }

        {
            let mut db = PersistentQueueWithCapacity::new(&path, 10, Options::default()).unwrap();
            let res = db.pop(1).unwrap();
            assert!(res.is_empty());
        }
        PersistentQueueWithCapacity::remove_db(&path).unwrap();
    }

    #[test]
    fn push_pop_many() {
        let path = "/tmp/test_push_pop_many".to_string();
        _ = PersistentQueueWithCapacity::remove_db(&path);
        let mut queue = PersistentQueueWithCapacity::new(&path, 1000, Options::default()).unwrap();
        queue
            .push(&[
                &[1u8, 2u8, 3u8],
                &[4u8, 5u8, 6u8],
                &[7u8, 8u8, 9u8],
                &[10u8, 11u8, 12u8],
            ])
            .unwrap();
        let res = queue.pop(3).unwrap();
        assert_eq!(
            res,
            vec![
                vec![1u8, 2u8, 3u8],
                vec![4u8, 5u8, 6u8],
                vec![7u8, 8u8, 9u8]
            ]
        );
        let res = queue.pop(3).unwrap();
        assert_eq!(res, vec![vec![10u8, 11u8, 12u8]]);

        let res = queue.pop(1).unwrap();
        assert!(res.is_empty());

        _ = PersistentQueueWithCapacity::remove_db(&path);
    }
}
