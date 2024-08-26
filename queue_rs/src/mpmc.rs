use std::cmp::Ordering;
use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Error, Result};
use bincode::config::Configuration;
use bincode::{Decode, Encode};
use rocksdb::{ColumnFamilyDescriptor, Direction, IteratorMode, Options, SliceTransform, DB};

use crate::utilities::{
    current_timestamp, index_to_key, key_to_index, next_index, previous_index, u64_from_byte_vec,
};
use crate::{fs, MAX_ALLOWED_INDEX};

const DATA_CF: &str = "data";
const SYSTEM_CF: &str = "system";
const READER_CF: &str = "reader";
const START_INDEX_KEY: u64 = u64::MAX;
const WRITE_INDEX_KEY: u64 = u64::MAX - 1;
const WRITE_TIMESTAMP_KEY: u64 = u64::MAX - 2;

#[derive(Clone, Copy)]
pub enum StartPosition {
    Oldest,
    Newest,
}

#[derive(Encode, Decode, PartialEq, Debug, Clone)]
struct Reader {
    index: u64,
    end_timestamp: Option<u64>,
    expired: bool,
}

impl Reader {
    fn new(index: u64, end_timestamp: Option<u64>, expired: bool) -> Self {
        Self {
            index,
            end_timestamp,
            expired,
        }
    }
}

pub struct MpmcQueue {
    db: DB,
    path: String,
    empty: bool,
    start_index: u64,
    write_index: u64,
    write_timestamp: u64,
    read_indices: HashMap<String, Reader>,
    configuration: Configuration,
}

impl MpmcQueue {
    pub fn new(path: &str, ttl: Duration) -> Result<Self> {
        let configuration = bincode::config::standard();

        let mut cf_opts = Options::default();
        cf_opts.create_if_missing(true);
        cf_opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(crate::U64_BYTE_LEN));
        let data_cf = ColumnFamilyDescriptor::new(DATA_CF, cf_opts);

        let mut cf_opts = Options::default();
        cf_opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(crate::U64_BYTE_LEN));
        let system_cf = ColumnFamilyDescriptor::new(SYSTEM_CF, cf_opts);

        let reader_cf = ColumnFamilyDescriptor::new(READER_CF, Options::default());

        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let db = DB::open_cf_descriptors_with_ttl(
            &db_opts,
            path,
            vec![system_cf, data_cf, reader_cf],
            ttl,
        )?;

        let system_cf = db.cf_handle(SYSTEM_CF).unwrap();
        let start_index_opt = db.get_cf(&system_cf, index_to_key(START_INDEX_KEY))?;
        let start_index = match start_index_opt {
            Some(v) => u64_from_byte_vec(&v),
            None => 0u64,
        };
        let write_index_opt = db.get_cf(&system_cf, index_to_key(WRITE_INDEX_KEY))?;
        let write_index = match write_index_opt {
            Some(v) => u64_from_byte_vec(&v),
            None => 0u64,
        };
        let write_timestamp_opt = db.get_cf(&system_cf, index_to_key(WRITE_TIMESTAMP_KEY))?;
        let write_timestamp = match write_timestamp_opt {
            Some(v) => u64_from_byte_vec(&v),
            None => current_timestamp(),
        };

        let data_cf = db.cf_handle(DATA_CF).unwrap();
        let mut empty = true;
        let iterator = db.iterator_cf(data_cf, IteratorMode::Start);
        // the error with iterator.next() - error[E0505]: cannot move out of `db` because it is borrowed
        #[allow(clippy::never_loop)]
        for item in iterator {
            item?;
            empty = false;
            break;
        }

        let mut read_indices = HashMap::new();
        let reader_cf = db.cf_handle(READER_CF).unwrap();
        let iterator = db.iterator_cf(reader_cf, IteratorMode::Start);
        for item in iterator {
            let (key, value) = item?;

            let key = String::from_utf8(Vec::from(key)).map_err(Error::from)?;
            let value = bincode::decode_from_slice(&value, configuration)?.0;

            read_indices.insert(key, value);
        }

        Ok(Self {
            db,
            path: path.to_string(),
            empty,
            start_index,
            write_index,
            write_timestamp,
            read_indices,
            configuration,
        })
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
            (match self.write_index.cmp(&self.start_index) {
                Ordering::Less => MAX_ALLOWED_INDEX - self.start_index + self.write_index,
                Ordering::Equal => MAX_ALLOWED_INDEX,
                Ordering::Greater => self.write_index - self.start_index,
            }) as usize
        }
    }

    pub fn is_empty(&self) -> bool {
        self.empty
    }

    pub fn add(&mut self, values: &[&[u8]]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        self.actualize_indices()?;
        if self.len() + values.len() > MAX_ALLOWED_INDEX as usize {
            return Err(anyhow::anyhow!("Queue is full"));
        }

        let data_cf = self.db.cf_handle(DATA_CF).unwrap();
        let system_cf = self.db.cf_handle(SYSTEM_CF).unwrap();
        let mut batch = rocksdb::WriteBatch::default();
        let mut write_index = self.write_index;

        for value in values {
            batch.put_cf(data_cf, index_to_key(write_index), value);
            write_index = next_index(write_index);
        }

        batch.put_cf(
            system_cf,
            index_to_key(WRITE_INDEX_KEY),
            write_index.to_le_bytes(),
        );
        let write_timestamp = current_timestamp();
        batch.put_cf(
            system_cf,
            index_to_key(WRITE_TIMESTAMP_KEY),
            write_timestamp.to_le_bytes(),
        );

        self.db.write(batch)?;
        self.write_index = write_index;
        self.write_timestamp = write_timestamp;
        self.empty = false;

        Ok(())
    }

    pub fn next(
        &mut self,
        max_elts: usize,
        label: &str,
        start_position: StartPosition,
    ) -> Result<(Vec<Vec<u8>>, bool)> {
        let mut res = Vec::with_capacity(max_elts);

        self.actualize_indices()?;
        let label = label.to_string();
        let data_cf = self.db.cf_handle(DATA_CF).unwrap();
        let reader_cf = self.db.cf_handle(READER_CF).unwrap();
        let mut reader = match self.read_indices.get(&label) {
            Some(e) => e.clone(),
            None => {
                let index = match start_position {
                    StartPosition::Oldest => self.start_index,
                    StartPosition::Newest => {
                        if self.empty {
                            self.write_index
                        } else {
                            previous_index(self.write_index)
                        }
                    }
                };
                Reader::new(index, None, false)
            }
        };

        let mut end = match reader.end_timestamp {
            None => reader.index == self.write_index && self.empty,
            Some(timestamp) => timestamp == self.write_timestamp,
        };

        while !end && res.len() < max_elts {
            let value = self.db.get_cf(data_cf, index_to_key(reader.index))?;
            if let Some(v) = value {
                res.push(v);
            } else {
                res.clear();
                reader.expired = true;
            }
            reader.index = next_index(reader.index);
            end = reader.index == self.write_index;
        }

        reader.end_timestamp = if end {
            Some(self.write_timestamp)
        } else {
            None
        };
        let expired = reader.expired;
        reader.expired = false;

        if !self.read_indices.get(&label).is_some_and(|e| *e == reader) {
            self.db.put_cf(
                reader_cf,
                label.as_bytes(),
                bincode::encode_to_vec(reader.clone(), self.configuration)?,
            )?;

            self.read_indices.insert(label, reader);
        }

        Ok((res, expired))
    }

    pub fn get_labels(&self) -> Vec<String> {
        self.read_indices
            .iter()
            .map(|e| e.0.clone())
            .collect::<Vec<String>>()
    }

    pub fn remove_label(&mut self, label: &str) -> Result<bool> {
        let label = label.to_string();
        if self.read_indices.contains_key(&label) {
            let reader_cf = self.db.cf_handle(READER_CF).unwrap();
            self.db.delete_cf(reader_cf, label.as_bytes())?;

            self.read_indices.remove(&label);

            return Ok(true);
        }
        Ok(false)
    }

    fn actualize_indices(&mut self) -> Result<()> {
        if self.empty {
            return Ok(());
        }

        let data_cf = self.db.cf_handle(DATA_CF).unwrap();
        let system_cf = self.db.cf_handle(SYSTEM_CF).unwrap();
        let reader_cf = self.db.cf_handle(READER_CF).unwrap();

        let mut iter = self.db.iterator_cf(
            data_cf,
            IteratorMode::From(&index_to_key(self.start_index), Direction::Forward),
        );
        let first_entry = if let Some(e) = iter.next() {
            Some(e)
        } else {
            // MAX_ALLOWED_INDEX handling
            let mut iter = self.db.iterator_cf(data_cf, IteratorMode::Start);
            iter.next()
        };

        let (start_index, empty, f) = match first_entry {
            Some(Err(e)) => return Err(anyhow::Error::from(e)),
            Some(Ok(e)) => {
                let start_index = key_to_index(e.0);
                if self.start_index == start_index {
                    // no elements have been expired
                    return Ok(());
                } else {
                    // some elements have been expired
                    let f: fn(u64, u64, u64, &mut Reader) =
                        |start, write_index, _write_timestamp, reader| {
                            let index = if reader.index == write_index {
                                reader.index
                            } else if reader.index > write_index {
                                if start > write_index {
                                    u64::max(start, reader.index)
                                } else {
                                    start
                                }
                            } else if start > write_index {
                                reader.index
                            } else {
                                u64::max(start, reader.index)
                            };
                            reader.expired = index != reader.index;
                            reader.index = index;
                        };
                    (start_index, false, f)
                }
            }
            None => {
                // all elements have been expired
                let f: fn(u64, u64, u64, &mut Reader) =
                    |_start, write_index, write_timestamp, reader| {
                        reader.expired = reader.end_timestamp != Some(write_timestamp);
                        reader.index = write_index;
                        reader.end_timestamp = Some(write_timestamp);
                    };
                (self.write_index, true, f)
            }
        };

        let mut batch = rocksdb::WriteBatch::default();

        for (label, reader) in self.read_indices.iter() {
            let mut reader = reader.clone();
            f(
                start_index,
                self.write_index,
                self.write_timestamp,
                &mut reader,
            );
            batch.put_cf(
                reader_cf,
                label.as_bytes(),
                bincode::encode_to_vec(reader, self.configuration)?,
            );
        }
        batch.put_cf(
            system_cf,
            index_to_key(START_INDEX_KEY),
            start_index.to_le_bytes(),
        );

        self.db.write(batch)?;

        self.start_index = start_index;
        self.empty = empty;
        self.read_indices
            .iter_mut()
            .for_each(|e| f(start_index, self.write_index, self.write_timestamp, e.1));

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::mpmc::{MpmcQueue, Reader, StartPosition, DATA_CF};
    use crate::utilities::{current_timestamp, index_to_key};
    use crate::MAX_ALLOWED_INDEX;
    use std::collections::HashMap;
    use std::fs;
    use std::ops::{Add, Div, Mul};
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    pub fn test_new_empty() {
        let path = std::env::temp_dir().join("empty");
        let path = path.to_str().unwrap();
        let _ = fs::remove_dir_all(path);

        let ttl = Duration::from_secs(60);
        let now = current_timestamp();

        let queue = MpmcQueue::new(path, ttl).unwrap();

        assert_eq!(queue.path, path);
        assert_eq!(queue.start_index, 0);
        assert_eq!(queue.write_index, 0);
        assert!(queue.write_timestamp > now);
        assert_eq!(queue.read_indices.is_empty(), true);
        assert_eq!(queue.empty, true);
        assert_eq!(queue.len(), 0);

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    pub fn test_new_non_empty() {
        let path = std::env::temp_dir().join("non-empty");
        let path = path.to_str().unwrap();
        let _ = fs::remove_dir_all(path);

        let ttl = Duration::from_secs(1);
        let values = vec!["a".as_bytes(), "b".as_bytes()];
        let label = "label";
        let write_timestamp = {
            let mut queue = MpmcQueue::new(path, ttl).unwrap();

            queue.add(&values).unwrap();
            queue.next(1, label, StartPosition::Oldest).unwrap();

            wait_and_expire(&mut queue, ttl.mul(2));

            queue.add(&values).unwrap();
            queue.next(1, label, StartPosition::Oldest).unwrap();

            assert_eq!(queue.start_index, 2);
            assert_eq!(queue.write_index, 4);
            assert_eq!(
                queue.read_indices,
                HashMap::from([(label.to_string(), Reader::new(3, None, false))])
            );
            assert_eq!(queue.empty, false);
            queue.write_timestamp
        };

        {
            let queue = MpmcQueue::new(path, ttl).unwrap();

            assert_eq!(queue.start_index, 2);
            assert_eq!(queue.write_index, 4);
            assert_eq!(queue.write_timestamp, write_timestamp);
            assert_eq!(
                queue.read_indices,
                HashMap::from([(label.to_string(), Reader::new(3, None, false))])
            );
            assert_eq!(queue.empty, false);
        }

        let _ = fs::remove_dir_all(path);
    }

    #[test]
    pub fn test_add() {
        test(Duration::from_secs(10), |mut queue| {
            queue.add(&["a".as_bytes()]).unwrap();

            assert_eq!(queue.start_index, 0);
            assert_eq!(queue.write_index, 1);
            assert_eq!(queue.read_indices.is_empty(), true);
            assert_eq!(queue.empty, false);
            assert_eq!(queue.len(), 1);
        });
    }

    #[test]
    pub fn test_add_too_big_batch() {
        test(Duration::from_secs(10), |mut queue| {
            let values = vec!["a".as_bytes(); (MAX_ALLOWED_INDEX + 1) as usize];
            let result = queue.add(&values);

            assert_eq!(result.is_err(), true);
            assert_eq!(queue.start_index, 0);
            assert_eq!(queue.write_index, 0);
            assert_eq!(queue.read_indices.is_empty(), true);
            assert_eq!(queue.empty, true);
            assert_eq!(queue.len(), 0);
        });
    }

    #[test]
    pub fn test_add_full_queue() {
        test(Duration::from_secs(10), |mut queue| {
            let values = vec!["a".as_bytes(); MAX_ALLOWED_INDEX as usize];
            queue.add(&values).unwrap();

            assert_eq!(queue.start_index, 0);
            assert_eq!(queue.write_index, 0);
            assert_eq!(queue.read_indices.is_empty(), true);
            assert_eq!(queue.empty, false);
            assert_eq!(queue.len(), MAX_ALLOWED_INDEX as usize);

            let result = queue.add(&["b".as_bytes()]);

            assert_eq!(result.is_err(), true);

            assert_eq!(queue.start_index, 0);
            assert_eq!(queue.write_index, 0);
            assert_eq!(queue.read_indices.is_empty(), true);
            assert_eq!(queue.empty, false);
            assert_eq!(queue.len(), MAX_ALLOWED_INDEX as usize);
        });
    }

    #[test]
    pub fn test_next_new_label_oldest_empty_queue() {
        let ttl = Duration::from_secs(60);
        let label = "label";

        test(ttl, |mut queue| {
            let result = queue.next(100, label, StartPosition::Oldest).unwrap();

            assert_eq!(result.0.is_empty(), true);
            assert_eq!(result.1, false);
            assert_eq!(
                queue.read_indices,
                HashMap::from([(
                    label.to_string(),
                    Reader::new(0, Some(queue.write_timestamp), false)
                )])
            );
        });
    }

    #[test]
    pub fn test_next_new_label_newest_empty_queue() {
        let ttl = Duration::from_secs(60);
        let label = "label";

        test(ttl, |mut queue| {
            let result = queue.next(100, label, StartPosition::Newest).unwrap();

            assert_eq!(result.0.is_empty(), true);
            assert_eq!(result.1, false);
            assert_eq!(
                queue.read_indices,
                HashMap::from([(
                    label.to_string(),
                    Reader::new(0, Some(queue.write_timestamp), false)
                )])
            );
        });
    }

    #[test]
    pub fn test_next_new_label_oldest_non_empty_queue() {
        let ttl = Duration::from_secs(60);
        let label = "label";
        let value_one = "a".as_bytes();
        let value_two = "b".as_bytes();
        let value_three = "c".as_bytes();
        let start_position = StartPosition::Oldest;

        test(ttl, |mut queue| {
            queue.add(&[value_one, value_two, value_three]).unwrap();

            let result = queue.next(2, label, start_position).unwrap();

            assert_eq!(result.0, vec![value_one.to_vec(), value_two.to_vec()]);
            assert_eq!(result.1, false);
            assert_eq!(
                queue.read_indices,
                HashMap::from([(label.to_string(), Reader::new(2, None, false))])
            );

            let result = queue.next(2, label, start_position).unwrap();

            assert_eq!(result.0, vec![value_three.to_vec()]);
            assert_eq!(result.1, false);
            assert_eq!(
                queue.read_indices,
                HashMap::from([(
                    label.to_string(),
                    Reader::new(3, Some(queue.write_timestamp), false)
                )])
            );
        });
    }

    #[test]
    pub fn test_next_new_label_newest_non_empty_queue() {
        let ttl = Duration::from_secs(60);
        let label = "label";
        let value_one = "a".as_bytes();
        let value_two = "b".as_bytes();
        let value_three = "c".as_bytes();
        let start_position = StartPosition::Newest;

        test(ttl, |mut queue| {
            queue.add(&[value_one, value_two, value_three]).unwrap();

            let result = queue.next(2, label, start_position).unwrap();

            assert_eq!(result.0, vec![value_three.to_vec()]);
            assert_eq!(result.1, false);
            assert_eq!(
                queue.read_indices,
                HashMap::from([(
                    label.to_string(),
                    Reader::new(3, Some(queue.write_timestamp), false)
                )])
            );

            let result = queue.next(2, label, start_position).unwrap();

            assert_eq!(result.0.is_empty(), true);
            assert_eq!(result.1, false);
            assert_eq!(
                queue.read_indices,
                HashMap::from([(
                    label.to_string(),
                    Reader::new(3, Some(queue.write_timestamp), false)
                )])
            );
        });
    }

    #[test]
    pub fn test_next_new_label_newest_full_queue() {
        let ttl = Duration::from_secs(1);
        let label = "label";
        let last_value = "last".as_bytes();

        test(ttl, |mut queue| {
            queue
                .add(&["value".as_bytes(); (MAX_ALLOWED_INDEX - 1) as usize])
                .unwrap();
            queue.add(&[last_value]).unwrap();

            let result = queue.next(1, label, StartPosition::Newest).unwrap();

            assert_eq!(result.0, vec![last_value.to_vec()]);
            assert_eq!(result.1, false);
            assert_eq!(
                queue.read_indices,
                HashMap::from([(
                    label.to_string(),
                    Reader::new(0, Some(queue.write_timestamp), false)
                )])
            );

            let result = queue.next(1, label, StartPosition::Newest).unwrap();

            assert_eq!(result.0.is_empty(), true);
            assert_eq!(result.1, false);
            assert_eq!(
                queue.read_indices,
                HashMap::from([(
                    label.to_string(),
                    Reader::new(0, Some(queue.write_timestamp), false)
                )])
            );
        });
    }

    #[test]
    pub fn test_next_new_label_oldest_all_expired() {
        let ttl = Duration::from_secs(1);
        let label_one = "label1";
        let label_two = "label2";

        test(ttl, |mut queue| {
            queue.add(&["a".as_bytes(), "b".as_bytes()]).unwrap();

            wait_and_expire(&mut queue, ttl.mul(2));

            let result = queue.next(2, label_one, StartPosition::Oldest).unwrap();

            assert_eq!(result.0.is_empty(), true);
            assert_eq!(result.1, false);
            assert_eq!(queue.start_index, 2);
            assert_eq!(queue.write_index, 2);
            assert_eq!(
                queue.read_indices,
                HashMap::from([(
                    label_one.to_string(),
                    Reader::new(2, Some(queue.write_timestamp), false)
                )])
            );
            assert_eq!(queue.empty, true);
            assert_eq!(queue.len(), 0);

            let write_timestamp = queue.write_timestamp;

            queue
                .add(&["a".as_bytes(); MAX_ALLOWED_INDEX as usize])
                .unwrap();

            assert_eq!(queue.start_index, 2);
            assert_eq!(queue.write_index, 2);
            assert_eq!(
                queue.read_indices,
                HashMap::from([(
                    label_one.to_string(),
                    Reader::new(2, Some(write_timestamp), false)
                )])
            );
            assert_eq!(queue.empty, false);
            assert_eq!(queue.len(), MAX_ALLOWED_INDEX as usize);
            assert!(queue.write_timestamp > write_timestamp);

            // expire all
            wait_and_expire(&mut queue, ttl.mul(2));

            let result = queue.next(1, label_two, StartPosition::Oldest).unwrap();

            assert_eq!(result.0.is_empty(), true);
            assert_eq!(result.1, false);
            assert_eq!(queue.start_index, 2);
            assert_eq!(queue.write_index, 2);
            assert_eq!(
                queue.read_indices,
                HashMap::from([
                    (
                        label_one.to_string(),
                        Reader::new(2, Some(queue.write_timestamp), true)
                    ),
                    (
                        label_two.to_string(),
                        Reader::new(2, Some(queue.write_timestamp), false)
                    )
                ])
            );
            assert_eq!(queue.empty, true);
            assert_eq!(queue.len(), 0);

            let result = queue.next(1, label_one, StartPosition::Oldest).unwrap();

            assert_eq!(result.0.is_empty(), true);
            assert_eq!(result.1, true);
            assert_eq!(
                queue.read_indices,
                HashMap::from([
                    (
                        label_one.to_string(),
                        Reader::new(2, Some(queue.write_timestamp), false)
                    ),
                    (
                        label_two.to_string(),
                        Reader::new(2, Some(queue.write_timestamp), false)
                    )
                ])
            );

            let result = queue.next(1, label_one, StartPosition::Oldest).unwrap();

            assert_eq!(result.0.is_empty(), true);
            assert_eq!(result.1, false);
            assert_eq!(
                queue.read_indices,
                HashMap::from([
                    (
                        label_one.to_string(),
                        Reader::new(2, Some(queue.write_timestamp), false)
                    ),
                    (
                        label_two.to_string(),
                        Reader::new(2, Some(queue.write_timestamp), false)
                    )
                ])
            );
        });
    }

    #[test]
    pub fn test_next_new_label_newest_all_expired() {
        let ttl = Duration::from_secs(1);
        let label_one = "label1";
        let label_two = "label2";

        test(ttl, |mut queue| {
            queue.add(&["a".as_bytes(), "b".as_bytes()]).unwrap();

            wait_and_expire(&mut queue, ttl.mul(2));

            let result = queue.next(2, label_one, StartPosition::Newest).unwrap();

            assert_eq!(result.0.is_empty(), true);
            assert_eq!(result.1, false);
            assert_eq!(queue.start_index, 2);
            assert_eq!(queue.write_index, 2);
            assert_eq!(
                queue.read_indices,
                HashMap::from([(
                    label_one.to_string(),
                    Reader::new(2, Some(queue.write_timestamp), false)
                )])
            );
            assert_eq!(queue.empty, true);
            assert_eq!(queue.len(), 0);

            let write_timestamp = queue.write_timestamp;

            queue
                .add(&["a".as_bytes(); MAX_ALLOWED_INDEX as usize])
                .unwrap();

            assert_eq!(queue.start_index, 2);
            assert_eq!(queue.write_index, 2);
            assert_eq!(
                queue.read_indices,
                HashMap::from([(
                    label_one.to_string(),
                    Reader::new(2, Some(write_timestamp), false)
                )])
            );
            assert_eq!(queue.empty, false);
            assert_eq!(queue.len(), MAX_ALLOWED_INDEX as usize);
            assert!(queue.write_timestamp > write_timestamp);

            // expire all
            wait_and_expire(&mut queue, ttl.mul(2));

            let result = queue.next(1, label_two, StartPosition::Newest).unwrap();

            assert_eq!(result.0.is_empty(), true);
            assert_eq!(result.1, false);
            assert_eq!(queue.start_index, 2);
            assert_eq!(queue.write_index, 2);
            assert_eq!(
                queue.read_indices,
                HashMap::from([
                    (
                        label_one.to_string(),
                        Reader::new(2, Some(queue.write_timestamp), true)
                    ),
                    (
                        label_two.to_string(),
                        Reader::new(2, Some(queue.write_timestamp), false)
                    )
                ])
            );
            assert_eq!(queue.empty, true);
            assert_eq!(queue.len(), 0);

            let result = queue.next(1, label_one, StartPosition::Oldest).unwrap();

            assert_eq!(result.0.is_empty(), true);
            assert_eq!(result.1, true);
            assert_eq!(
                queue.read_indices,
                HashMap::from([
                    (
                        label_one.to_string(),
                        Reader::new(2, Some(queue.write_timestamp), false)
                    ),
                    (
                        label_two.to_string(),
                        Reader::new(2, Some(queue.write_timestamp), false)
                    )
                ])
            );

            let result = queue.next(1, label_one, StartPosition::Oldest).unwrap();

            assert_eq!(result.0.is_empty(), true);
            assert_eq!(result.1, false);
            assert_eq!(
                queue.read_indices,
                HashMap::from([
                    (
                        label_one.to_string(),
                        Reader::new(2, Some(queue.write_timestamp), false)
                    ),
                    (
                        label_two.to_string(),
                        Reader::new(2, Some(queue.write_timestamp), false)
                    )
                ])
            );
        });
    }

    #[test]
    pub fn test_next_new_label_oldest_some_expired() {
        let ttl = Duration::from_secs(1);
        let label = "label";
        let value = "c".as_bytes();

        test(ttl, |mut queue| {
            queue.add(&["a".as_bytes()]).unwrap();

            sleep(ttl.mul(2));

            queue.add(&[value, "b".as_bytes()]).unwrap();

            let data_cf = queue.db.cf_handle(DATA_CF).unwrap();
            queue
                .db
                .compact_range_cf(data_cf, None::<&[u8]>, None::<&[u8]>);

            let result = queue.next(1, label, StartPosition::Oldest).unwrap();

            assert_eq!(result.0, vec![value]);
            assert_eq!(result.1, false);
            assert_eq!(queue.start_index, 1);
            assert_eq!(queue.write_index, 3);
            assert_eq!(
                queue.read_indices,
                HashMap::from([(label.to_string(), Reader::new(2, None, false))])
            );
            assert_eq!(queue.empty, false);
            assert_eq!(queue.len(), 2);
        });
    }

    #[test]
    pub fn test_next_new_label_newest_some_expired() {
        let ttl = Duration::from_secs(1);
        let label = "label";
        let value = "c".as_bytes();

        test(ttl, |mut queue| {
            queue.add(&["a".as_bytes()]).unwrap();

            sleep(ttl.mul(2));

            queue.add(&["b".as_bytes(), value]).unwrap();

            let data_cf = queue.db.cf_handle(DATA_CF).unwrap();
            queue
                .db
                .compact_range_cf(data_cf, None::<&[u8]>, None::<&[u8]>);

            let result = queue.next(1, label, StartPosition::Newest).unwrap();

            assert_eq!(result.0, vec![value]);
            assert_eq!(result.1, false);
            assert_eq!(queue.start_index, 1);
            assert_eq!(queue.write_index, 3);
            assert_eq!(
                queue.read_indices,
                HashMap::from([(
                    label.to_string(),
                    Reader::new(3, Some(queue.write_timestamp), false)
                )])
            );
            assert_eq!(queue.empty, false);
            assert_eq!(queue.len(), 2);
        });
    }

    #[test]
    pub fn test_next_after_ttl_start_index_less_write_index() {
        let quarter_ttl = Duration::from_secs(1);
        let ttl = quarter_ttl.mul(4);
        let label_one = "label1";
        let label_two = "label2";
        let label_three = "label3";
        let label_four = "label4";
        let label_five = "label5";
        let value_one = "a".as_bytes();
        let value_two = "b".as_bytes();
        let value_three = "c".as_bytes();
        let value_four = "d".as_bytes();
        let start_position = StartPosition::Oldest;

        test(ttl, |mut queue| {
            queue.add(&[value_one, value_two]).unwrap();

            wait_and_expire(&mut queue, quarter_ttl.mul(3));

            queue.add(&[value_three, value_four]).unwrap();

            // read < write, start > read
            let result = queue.next(1, label_one, start_position).unwrap();
            assert_eq!(result.0, vec![value_one]);
            assert_eq!(result.1, false);
            // read < write, start = read
            let result = queue.next(2, label_two, start_position).unwrap();
            assert_eq!(result.0, vec![value_one, value_two]);
            assert_eq!(result.1, false);
            // read < write, start < read
            let result = queue.next(3, label_three, start_position).unwrap();
            assert_eq!(result.0, vec![value_one, value_two, value_three]);
            assert_eq!(result.1, false);
            // read = write
            let result = queue.next(4, label_four, start_position).unwrap();
            assert_eq!(
                result.0,
                vec![value_one, value_two, value_three, value_four]
            );
            assert_eq!(result.1, false);

            assert_eq!(queue.start_index, 0);
            assert_eq!(queue.write_index, 4);
            assert_eq!(
                queue.read_indices,
                HashMap::from([
                    (label_one.to_string(), Reader::new(1, None, false)),
                    (label_two.to_string(), Reader::new(2, None, false)),
                    (label_three.to_string(), Reader::new(3, None, false)),
                    (
                        label_four.to_string(),
                        Reader::new(4, Some(queue.write_timestamp), false)
                    )
                ])
            );
            assert_eq!(queue.empty, false);
            assert_eq!(queue.len(), 4);

            wait_and_expire(&mut queue, quarter_ttl.mul(2));

            let result = queue.next(1, label_five, start_position).unwrap();
            assert_eq!(result.0, vec![value_three]);
            assert_eq!(result.1, false);

            assert_eq!(queue.start_index, 2);
            assert_eq!(queue.write_index, 4);
            assert_eq!(
                queue.read_indices,
                HashMap::from([
                    (label_one.to_string(), Reader::new(2, None, true)),
                    (label_two.to_string(), Reader::new(2, None, false)),
                    (label_three.to_string(), Reader::new(3, None, false)),
                    (
                        label_four.to_string(),
                        Reader::new(4, Some(queue.write_timestamp), false)
                    ),
                    (label_five.to_string(), Reader::new(3, None, false))
                ])
            );
            assert_eq!(queue.empty, false);
            assert_eq!(queue.len(), 2);

            wait_and_expire(&mut queue, quarter_ttl.mul(3));

            let result = queue.next(1, label_five, start_position).unwrap();
            assert_eq!(result.0.is_empty(), true);
            assert_eq!(result.1, true);

            assert_eq!(queue.start_index, 4);
            assert_eq!(queue.write_index, 4);
            assert_eq!(
                queue.read_indices,
                HashMap::from([
                    (
                        label_one.to_string(),
                        Reader::new(4, Some(queue.write_timestamp), true)
                    ),
                    (
                        label_two.to_string(),
                        Reader::new(4, Some(queue.write_timestamp), true)
                    ),
                    (
                        label_three.to_string(),
                        Reader::new(4, Some(queue.write_timestamp), true)
                    ),
                    (
                        label_four.to_string(),
                        Reader::new(4, Some(queue.write_timestamp), false)
                    ),
                    (
                        label_five.to_string(),
                        Reader::new(4, Some(queue.write_timestamp), false)
                    )
                ])
            );
            assert_eq!(queue.empty, true);
            assert_eq!(queue.len(), 0);
        });
    }

    #[test]
    pub fn test_next_after_ttl_start_index_greater_write_index() {
        let quarter_ttl = Duration::from_secs(1);
        let ttl = quarter_ttl.mul(4);
        let label_one = "label1";
        let label_two = "label2";
        let label_three = "label3";
        let label_four = "label4";
        let label_five = "label5";
        let label_six = "label6";
        let value_one = "a".as_bytes();
        let value_two = "b".as_bytes();
        let value_three = "c".as_bytes();
        let value_four = "d".as_bytes();
        let value_five = "e".as_bytes();
        let value_six = "f".as_bytes();
        let start_position = StartPosition::Oldest;

        test(ttl, |mut queue| {
            queue.add(&[value_one, value_two]).unwrap();

            // expire all to emulate write index < start index
            wait_and_expire(&mut queue, quarter_ttl.mul(5));

            let result = queue.next(1, label_one, start_position).unwrap();
            assert_eq!(result.0.is_empty(), true);
            assert_eq!(result.1, false);

            assert_eq!(queue.start_index, 2);
            assert_eq!(queue.write_index, 2);
            assert_eq!(
                queue.read_indices,
                HashMap::from([(
                    label_one.to_string(),
                    Reader::new(2, Some(queue.write_timestamp), false)
                )])
            );
            assert_eq!(queue.empty, true);
            assert_eq!(queue.len(), 0);

            queue.add(&[value_one, value_two]).unwrap();

            // value one and value two will expire than elements added later
            sleep(quarter_ttl.mul(3));

            queue
                .add(&[value_three, value_four, value_five, value_six])
                .unwrap();
            // read > write, start > read
            let result = queue.next(1, label_one, start_position).unwrap();
            assert_eq!(result.0, vec![value_one]);
            assert_eq!(result.1, false);
            // read > write, start = read
            let result = queue.next(2, label_two, start_position).unwrap();
            assert_eq!(result.0, vec![value_one, value_two]);
            assert_eq!(result.1, false);
            // read > write, start < read
            let result = queue.next(3, label_three, start_position).unwrap();
            assert_eq!(result.0, vec![value_one, value_two, value_three]);
            assert_eq!(result.1, false);
            // read < write, start > read
            let result = queue.next(4, label_four, start_position).unwrap();
            assert_eq!(
                result.0,
                vec![value_one, value_two, value_three, value_four]
            );
            assert_eq!(result.1, false);
            let result = queue.next(5, label_five, start_position).unwrap();
            assert_eq!(
                result.0,
                vec![value_one, value_two, value_three, value_four, value_five]
            );
            assert_eq!(result.1, false);
            // read == write
            let result = queue.next(6, label_six, start_position).unwrap();
            assert_eq!(
                result.0,
                vec![
                    value_one,
                    value_two,
                    value_three,
                    value_four,
                    value_five,
                    value_six
                ]
            );
            assert_eq!(result.1, false);

            assert_eq!(queue.start_index, 2);
            assert_eq!(queue.write_index, 2);
            assert_eq!(
                queue.read_indices,
                HashMap::from([
                    (label_one.to_string(), Reader::new(3, None, false)),
                    (label_two.to_string(), Reader::new(4, None, false)),
                    (label_three.to_string(), Reader::new(5, None, false)),
                    (label_four.to_string(), Reader::new(0, None, false)),
                    (label_five.to_string(), Reader::new(1, None, false)),
                    (
                        label_six.to_string(),
                        Reader::new(2, Some(queue.write_timestamp), false)
                    )
                ])
            );
            assert_eq!(queue.empty, false);
            assert_eq!(queue.len(), 6);

            // expire value one and value two
            wait_and_expire(&mut queue, quarter_ttl.mul(2));

            let result = queue.next(1, label_five, start_position).unwrap();
            assert_eq!(result.0, vec![value_six]);
            assert_eq!(result.1, false);

            assert_eq!(queue.start_index, 4);
            assert_eq!(queue.write_index, 2);
            assert_eq!(
                queue.read_indices,
                HashMap::from([
                    (label_one.to_string(), Reader::new(4, None, true)),
                    (label_two.to_string(), Reader::new(4, None, false)),
                    (label_three.to_string(), Reader::new(5, None, false)),
                    (label_four.to_string(), Reader::new(0, None, false)),
                    (
                        label_five.to_string(),
                        Reader::new(2, Some(queue.write_timestamp), false)
                    ),
                    (
                        label_six.to_string(),
                        Reader::new(2, Some(queue.write_timestamp), false)
                    )
                ])
            );
            assert_eq!(queue.empty, false);
            assert_eq!(queue.len(), 4);

            // expire all elements
            wait_and_expire(&mut queue, quarter_ttl.mul(3));

            let result = queue.next(1, label_five, start_position).unwrap();
            assert_eq!(result.0.is_empty(), true);
            assert_eq!(result.1, false);

            assert_eq!(queue.start_index, 2);
            assert_eq!(queue.write_index, 2);
            assert_eq!(
                queue.read_indices,
                HashMap::from([
                    (
                        label_one.to_string(),
                        Reader::new(2, Some(queue.write_timestamp), true)
                    ),
                    (
                        label_two.to_string(),
                        Reader::new(2, Some(queue.write_timestamp), true)
                    ),
                    (
                        label_three.to_string(),
                        Reader::new(2, Some(queue.write_timestamp), true)
                    ),
                    (
                        label_four.to_string(),
                        Reader::new(2, Some(queue.write_timestamp), true)
                    ),
                    (
                        label_five.to_string(),
                        Reader::new(2, Some(queue.write_timestamp), false)
                    ),
                    (
                        label_six.to_string(),
                        Reader::new(2, Some(queue.write_timestamp), false)
                    )
                ])
            );
            assert_eq!(queue.empty, true);
            assert_eq!(queue.len(), 0);
        });
    }

    #[test]
    pub fn test_next_after_ttl_start_index_greater_write_index_reverse() {
        let ttl = Duration::from_secs(4);
        let label_one = "label1";
        let label_two = "label2";

        test(ttl, |mut queue| {
            queue.add(&["a".as_bytes(); 2]).unwrap();

            // expire all to emulate write index < start index
            wait_and_expire(&mut queue, ttl.add(Duration::from_secs(1)));

            queue
                .add(&[
                    "a".as_bytes(),
                    "b".as_bytes(),
                    "c".as_bytes(),
                    "d".as_bytes(),
                ])
                .unwrap();
            queue.next(3, label_one, StartPosition::Oldest).unwrap();

            assert_eq!(queue.start_index, 2);
            assert_eq!(queue.write_index, 0);
            assert_eq!(
                queue.read_indices,
                HashMap::from([(label_one.to_string(), Reader::new(5, None, false))])
            );
            assert_eq!(queue.empty, false);
            assert_eq!(queue.len(), 4);

            sleep(ttl.mul_f32(0.75));

            queue.add(&["e".as_bytes(), "f".as_bytes()]).unwrap();

            assert_eq!(queue.start_index, 2);
            assert_eq!(queue.write_index, 2);
            assert_eq!(
                queue.read_indices,
                HashMap::from([(label_one.to_string(), Reader::new(5, None, false))])
            );
            assert_eq!(queue.empty, false);
            assert_eq!(queue.len(), 6);

            wait_and_expire(&mut queue, ttl.div(2));

            queue.next(1, label_two, StartPosition::Oldest).unwrap();

            assert_eq!(queue.start_index, 0);
            assert_eq!(queue.write_index, 2);
            assert_eq!(
                queue.read_indices,
                HashMap::from([
                    (label_one.to_string(), Reader::new(0, None, true)),
                    (label_two.to_string(), Reader::new(1, None, false))
                ])
            );
            assert_eq!(queue.empty, false);
            assert_eq!(queue.len(), 2);
        });
    }

    #[test]
    pub fn test_next_during_expiration_some_elements() {
        let ttl = Duration::from_secs(4);
        let label = "label";
        let value_three = "c".as_bytes();
        let value_four = "d".as_bytes();

        test(ttl, |mut queue| {
            queue
                .add(&["a".as_bytes(), "b".as_bytes(), value_three, value_four])
                .unwrap();

            // emulate that the second value expires right after reading the first value
            let data_cf = queue.db.cf_handle(DATA_CF).unwrap();
            queue.db.delete_cf(data_cf, index_to_key(1)).unwrap();

            let result = queue.next(2, label, StartPosition::Oldest).unwrap();

            assert_eq!(result.0, vec![value_three, value_four]);
            assert_eq!(result.1, true);
            assert_eq!(queue.start_index, 0);
            assert_eq!(queue.write_index, 4);
            assert_eq!(
                queue.read_indices,
                HashMap::from([(
                    label.to_string(),
                    Reader::new(4, Some(queue.write_timestamp), false)
                )])
            );
            assert_eq!(queue.empty, false);
            assert_eq!(queue.len(), 4);
        });
    }

    #[test]
    pub fn test_next_during_expiration_all_elements() {
        let ttl = Duration::from_secs(4);
        let label = "label";
        let value_three = "c".as_bytes();
        let value_four = "d".as_bytes();

        test(ttl, |mut queue| {
            queue
                .add(&["a".as_bytes(), "b".as_bytes(), value_three, value_four])
                .unwrap();

            // emulate that the all values expire right after reading the first value
            let data_cf = queue.db.cf_handle(DATA_CF).unwrap();
            queue.db.delete_cf(data_cf, index_to_key(1)).unwrap();
            queue.db.delete_cf(data_cf, index_to_key(2)).unwrap();
            queue.db.delete_cf(data_cf, index_to_key(3)).unwrap();

            let result = queue.next(4, label, StartPosition::Oldest).unwrap();

            assert_eq!(result.0.is_empty(), true);
            assert_eq!(result.1, true);
            assert_eq!(queue.start_index, 0);
            assert_eq!(queue.write_index, 4);
            assert_eq!(
                queue.read_indices,
                HashMap::from([(
                    label.to_string(),
                    Reader::new(4, Some(queue.write_timestamp), false)
                )])
            );
            assert_eq!(queue.empty, false);
            assert_eq!(queue.len(), 4);
        });
    }

    #[test]
    pub fn test_get_labels() {
        let label_one = "label1";
        let label_two = "label2";
        test(Duration::from_secs(10), |mut queue| {
            let result = queue.get_labels();

            assert_eq!(result.is_empty(), true);

            queue.next(1, label_one, StartPosition::Oldest).unwrap();
            let result = queue.get_labels();

            assert_eq!(result, vec![label_one.to_string()]);

            queue.next(1, label_two, StartPosition::Oldest).unwrap();
            let result = queue.get_labels();

            assert_eq!(result.len(), 2);
            assert_eq!(result.contains(&label_one.to_string()), true);
            assert_eq!(result.contains(&label_two.to_string()), true);
        });
    }

    #[test]
    pub fn test_remove_label() {
        let label = "label";
        test(Duration::from_secs(10), |mut queue| {
            let result = queue.remove_label(label).unwrap();

            assert_eq!(result, false);
            assert_eq!(queue.read_indices.is_empty(), true);

            queue.next(1, label, StartPosition::Oldest).unwrap();
            assert_eq!(
                queue.read_indices,
                HashMap::from([(
                    label.to_string(),
                    Reader::new(0, Some(queue.write_timestamp), false)
                )])
            );

            let result = queue.remove_label(label).unwrap();

            assert_eq!(result, true);
            assert_eq!(queue.read_indices.is_empty(), true);
        });
    }

    fn test<F>(ttl: Duration, mut f: F)
    where
        F: FnMut(MpmcQueue),
    {
        let directory = tempfile::TempDir::new().unwrap();
        let path = directory.path().to_str().unwrap();

        let queue = MpmcQueue::new(path, ttl).unwrap();

        f(queue);
    }

    fn wait_and_expire(queue: &mut MpmcQueue, duration: Duration) {
        sleep(duration);
        let data_cf = queue.db.cf_handle(DATA_CF).unwrap();
        queue
            .db
            .compact_range_cf(data_cf, None::<&[u8]>, None::<&[u8]>);
    }
}
