use crate::prelude::*;
use std::sync::Arc;

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;

use crate::data::log_record::LogRecordPos;

use super::{skiplist_iterator::SkipListIterator, Indexer};

pub struct SkipList {
    skl: Arc<SkipMap<Vec<u8>, LogRecordPos>>,
}

impl SkipList {
    pub fn new() -> Self {
        Self {
            skl: Arc::new(SkipMap::new()),
        }
    }
}

impl Indexer for SkipList {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> Option<LogRecordPos> {
        let mut old_value = None;

        if let Some(entry) = self.skl.get(&key) {
            old_value = Some(*entry.value());
        }
        self.skl.insert(key, pos);
        old_value
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        if let Some(entry) = self.skl.get(&key) {
            return Some(*entry.value());
        }
        None
    }

    fn delete(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        if let Some(entry) = self.skl.remove(&key) {
            return Some(*entry.value());
        }
        None
    }

    fn iterator(&self, options: crate::options::IteratorOptions) -> Box<dyn super::IndexIterator> {
        let mut items = Vec::with_capacity(self.skl.len());

        for entry in self.skl.iter() {
            let (key, value) = (entry.key(), entry.value());
            items.push((key.clone(), value.clone()));
        }

        if options.reverse {
            items.reverse();
        }

        Box::new(SkipListIterator {
            items,
            curr_index: 0,
            options,
        })
    }

    fn list_keys(&self) -> Result<Vec<bytes::Bytes>> {
        let mut keys = Vec::with_capacity(self.skl.len());
        for entry in self.skl.iter() {
            keys.push(Bytes::copy_from_slice(entry.key()));
        }

        Ok(keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_put() {
        let bt = SkipList::new();
        let ret1 = bt.put(
            "ret1".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 32,
                size: 100,
            },
        );

        assert_eq!(ret1.is_none(), true);

        let ret2 = bt.put(
            "ret2".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 32,
                size: 100,
            },
        );

        assert_eq!(ret2.is_none(), true);

        let ret1 = bt.put(
            "ret1".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 3,
                offset: 4,
                size: 100,
            },
        );
        assert_eq!(ret1.is_some(), true);
        let old_pos = ret1.unwrap();
        assert_eq!(old_pos.file_id, 1);
        assert_eq!(old_pos.offset, 32);
    }

    #[test]
    fn test_btree_get_exist_key() {
        let bt = SkipList::new();
        let ret1 = bt.put(
            "ret1".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 32,
                size: 100,
            },
        );
        assert_eq!(ret1.is_none(), true);

        let pos = bt.get("ret1".as_bytes().to_vec());
        assert!(pos.is_some());
        let pos = pos.unwrap();
        assert_eq!(pos.file_id, 1);
        assert_eq!(pos.offset, 32);
    }

    #[test]
    fn test_btree_get_non_exist_key() {
        let bt = SkipList::new();
        let pos1 = bt.get("ret1".as_bytes().to_vec());
        assert!(pos1.is_none());

        let pos2: Option<LogRecordPos> = bt.get("".as_bytes().to_vec());
        assert!(pos2.is_none());
    }

    #[test]
    fn test_btree_delete_exist_key() {
        let bt = SkipList::new();
        let ret1 = bt.put(
            "ret1".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 32,
                size: 100,
            },
        );
        assert_eq!(ret1.is_none(), true);

        let delete_ret = bt.delete("ret1".as_bytes().to_vec());
        assert_eq!(delete_ret.is_some(), true);
        let delete_pos = delete_ret.unwrap();
        assert_eq!(delete_pos.file_id, 1);
        assert_eq!(delete_pos.offset, 32);

        let pos1 = bt.get("ret1".as_bytes().to_vec());
        assert!(pos1.is_none());
    }

    #[test]
    fn test_btree_delete_non_exist_key() {
        let bt = SkipList::new();

        let delete_ret = bt.delete("ret1".as_bytes().to_vec());
        assert_eq!(delete_ret.is_none(), true);

        let pos1 = bt.get("ret1".as_bytes().to_vec());
        assert!(pos1.is_none());
    }
}
