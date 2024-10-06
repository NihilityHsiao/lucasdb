use crate::prelude::*;
use std::{collections::BTreeMap, sync::Arc};

use bytes::Bytes;
use parking_lot::RwLock;

use crate::data::log_record::LogRecordPos;

use super::{btree_iterator::BTreeIterator, IndexIterator, Indexer};

/// `BTree` 内存索引,封装了标准库的 `BTreeMap`
pub struct BTree {
    tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecordPos>>>,
}

impl BTree {
    pub fn new() -> Self {
        Self {
            tree: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl Indexer for BTree {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> Option<LogRecordPos> {
        let mut write_guard = self.tree.write();
        write_guard.insert(key, pos)
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let read_guard = self.tree.read();
        read_guard.get(&key).copied()
    }
    /// 删除key,key不存在返回false
    fn delete(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let mut write_guard = self.tree.write();
        write_guard.remove(&key)
    }

    fn iterator(&self, options: crate::options::IteratorOptions) -> Box<dyn IndexIterator> {
        let read_guard = self.tree.read();
        let mut items = Vec::with_capacity(read_guard.len());
        for (key, value) in read_guard.iter() {
            items.push((key.clone(), value.clone()));
        }

        if options.reverse {
            items.reverse();
        }
        Box::new(BTreeIterator {
            items,
            curr_index: 0,
            options,
        })
    }

    fn list_keys(&self) -> Result<Vec<bytes::Bytes>> {
        let read_guard = self.tree.read();
        let mut keys = Vec::with_capacity(read_guard.len());
        for (k, _) in read_guard.iter() {
            keys.push(Bytes::copy_from_slice(&k));
        }

        Ok(keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_put() {
        let bt = BTree::new();
        let ret1 = bt.put(
            "ret1".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 32,
            },
        );

        assert_eq!(true, ret1.is_none());

        let ret2 = bt.put(
            "ret2".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 32,
            },
        );

        assert_eq!(true, ret2.is_none());

        let ret1 = bt.put(
            "ret1".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 32,
            },
        );
        assert_eq!(true, ret1.is_some());
        let pos = ret1.unwrap();
        assert_eq!(1, pos.file_id);
        assert_eq!(32, pos.offset);
    }

    #[test]
    fn test_btree_get_exist_key() {
        let bt = BTree::new();
        let ret1 = bt.put(
            "ret1".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 32,
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
        let bt = BTree::new();
        let pos1 = bt.get("ret1".as_bytes().to_vec());
        assert!(pos1.is_none());

        let pos2: Option<LogRecordPos> = bt.get("".as_bytes().to_vec());
        assert!(pos2.is_none());
    }

    #[test]
    fn test_btree_delete_exist_key() {
        let bt = BTree::new();
        let ret1 = bt.put(
            "ret1".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 32,
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
        let bt = BTree::new();

        let delete_ret = bt.delete("ret1".as_bytes().to_vec());
        assert_eq!(delete_ret.is_none(), true);

        let pos1 = bt.get("ret1".as_bytes().to_vec());
        assert!(pos1.is_none());
    }
}
