pub mod blocking;
mod fs;
pub mod nonblocking;

use anyhow::{anyhow, Result};
use rocksdb::{Options, DB};
use std::cmp::Ordering;

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
    empty: bool,
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
        if max_elements > MAX_ALLOWED_INDEX as usize {
            return Err(anyhow!(
                "max_elements can't be greater than {}",
                MAX_ALLOWED_INDEX
            ));
        }
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

        let empty = db.get(Self::index_to_key(read_index))?.is_none();

        Ok(Self {
            db,
            path: path.to_string(),
            write_index,
            read_index,
            space_stat,
            max_elements: max_elements as u64,
            empty,
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
        if self.empty {
            0
        } else {
            (match self.write_index.cmp(&self.read_index) {
                Ordering::Less => MAX_ALLOWED_INDEX - self.read_index + self.write_index,
                Ordering::Equal => MAX_ALLOWED_INDEX,
                Ordering::Greater => self.write_index - self.read_index,
            }) as usize
        }
    }

    pub fn payload_size(&self) -> u64 {
        self.space_stat
    }

    pub fn is_empty(&self) -> bool {
        self.empty
    }

    pub fn push(&mut self, values: &[&[u8]]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        if self.len() + values.len() > self.max_elements as usize {
            return Err(anyhow::anyhow!("Queue is full"));
        }

        let mut batch = rocksdb::WriteBatch::default();
        let mut write_index = self.write_index;

        for value in values {
            batch.put(Self::index_to_key(write_index), value);
            write_index += 1;
            if write_index == MAX_ALLOWED_INDEX {
                write_index = 0;
            }
        }

        batch.put(
            Self::index_to_key(WRITE_INDEX_CELL),
            write_index.to_le_bytes(),
        );

        let space_stat = self.space_stat + values.iter().map(|v| v.len() as u64).sum::<u64>();

        batch.put(
            Self::index_to_key(SPACE_STAT_CELL),
            space_stat.to_le_bytes(),
        );

        self.db.write(batch)?;

        self.empty = false;
        self.write_index = write_index;
        self.space_stat = space_stat;

        Ok(())
    }

    pub fn pop(&mut self, mut max_elts: usize) -> Result<Vec<Vec<u8>>> {
        let mut res = Vec::with_capacity(max_elts);
        let mut batch = rocksdb::WriteBatch::default();
        let mut read_index = self.read_index;
        loop {
            let key = Self::index_to_key(read_index);
            let value = self.db.get(key)?;
            if let Some(v) = value {
                batch.delete(key);
                res.push(v);
                read_index += 1;
                if read_index == MAX_ALLOWED_INDEX {
                    read_index = 0;
                }
                max_elts -= 1;
            } else {
                break;
            }

            if read_index != self.write_index && max_elts > 0 {
                continue;
            } else {
                break;
            }
        }
        if !res.is_empty() {
            let empty = read_index == self.write_index;
            let space_stat = self.space_stat - res.iter().map(|v| v.len() as u64).sum::<u64>();
            batch.put(
                Self::index_to_key(SPACE_STAT_CELL),
                space_stat.to_le_bytes(),
            );
            batch.put(
                Self::index_to_key(READ_INDEX_CELL),
                read_index.to_le_bytes(),
            );
            self.db.write(batch)?;

            self.read_index = read_index;
            self.space_stat = space_stat;
            self.empty = empty;
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
            let mut db = PersistentQueueWithCapacity::new(
                &path,
                MAX_ALLOWED_INDEX as usize,
                Options::default(),
            )
            .unwrap();
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
            assert_eq!(db.len(), 0);
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
        let size = MAX_ALLOWED_INDEX as usize;
        {
            let mut db = PersistentQueueWithCapacity::new(&path, size, Options::default()).unwrap();
            db.push(&[&[1, 2, 3]]).unwrap();
            db.push(&[&[4, 5, 6]]).unwrap();
            db.push(&[&[7, 8, 9]]).unwrap();
        }

        {
            let mut db = PersistentQueueWithCapacity::new(&path, size, Options::default()).unwrap();
            assert_eq!(db.payload_size(), 9);
            let res = db.pop(1).unwrap();
            assert_eq!(res, vec![vec![1, 2, 3]]);
            assert_eq!(db.payload_size(), 6);
        }

        {
            let mut db = PersistentQueueWithCapacity::new(&path, size, Options::default()).unwrap();
            let res = db.pop(1).unwrap();
            assert_eq!(res, vec![vec![4, 5, 6]]);
            let res = db.pop(1).unwrap();
            assert_eq!(res, vec![vec![7, 8, 9]]);
        }

        {
            let mut db = PersistentQueueWithCapacity::new(&path, size, Options::default()).unwrap();
            let res = db.pop(1).unwrap();
            assert!(res.is_empty());
        }
        PersistentQueueWithCapacity::remove_db(&path).unwrap();
    }

    #[test]
    fn push_pop_many() {
        let path = "/tmp/test_push_pop_many".to_string();
        _ = PersistentQueueWithCapacity::remove_db(&path);
        let mut queue = PersistentQueueWithCapacity::new(&path, 3, Options::default()).unwrap();
        queue
            .push(&[&[1u8, 2u8, 3u8], &[4u8, 5u8, 6u8], &[7u8, 8u8, 9u8]])
            .unwrap();

        let res = queue.pop(2).unwrap();

        assert_eq!(res, vec![vec![1u8, 2u8, 3u8], vec![4u8, 5u8, 6u8]]);
        let res = queue.pop(1).unwrap();
        assert_eq!(res, vec![vec![7u8, 8u8, 9u8]]);

        let res = queue.pop(1).unwrap();
        assert!(res.is_empty());

        _ = PersistentQueueWithCapacity::remove_db(&path);
    }

    #[test]
    fn push_pop_max_elements() {
        let path = "/tmp/test_push_pop_max_elements".to_string();
        _ = PersistentQueueWithCapacity::remove_db(&path);
        let size = MAX_ALLOWED_INDEX as usize;
        let mut queue = PersistentQueueWithCapacity::new(&path, size, Options::default()).unwrap();

        let values = vec!["a".as_bytes(); size];
        queue.push(&values).unwrap();

        assert_eq!(queue.read_index, 0);
        assert_eq!(queue.write_index, 0);
        assert_eq!(queue.empty, false);
        assert_eq!(queue.len(), size);

        let res = queue.pop(size).unwrap();

        assert_eq!(res, values);
        assert_eq!(queue.read_index, 0);
        assert_eq!(queue.write_index, 0);
        assert_eq!(queue.empty, true);
        assert_eq!(queue.len(), 0);

        _ = PersistentQueueWithCapacity::remove_db(&path);
    }

    #[test]
    fn new_invalid_max_elements() {
        let result = PersistentQueueWithCapacity::new(
            "/tmp/test_invalid_max_elements",
            (MAX_ALLOWED_INDEX + 1) as usize,
            Options::default(),
        );

        assert_eq!(
            result.is_err_and(|e| e.to_string()
                == format!("max_elements can't be greater than {}", MAX_ALLOWED_INDEX)),
            true
        );
    }
}
