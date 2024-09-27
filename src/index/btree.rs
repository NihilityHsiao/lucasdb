use std::{collections::BTreeMap, sync::Arc};

use parking_lot::RwLock;

use crate::data::log_record::LogRecordPos;

use super::Indexer;

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
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool {
        let mut write_guard = self.tree.write();
        write_guard.insert(key, pos);
        true
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let read_guard = self.tree.read();
        read_guard.get(&key).copied()
    }
    /// 删除key,key不存在返回false
    fn delete(&self, key: Vec<u8>) -> bool {
        let mut write_guard = self.tree.write();
        let remove_res = write_guard.remove(&key);
        remove_res.is_some()
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

        assert_eq!(ret1, true);

        let ret2 = bt.put(
            "ret2".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 32,
            },
        );

        assert_eq!(ret2, true);
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
        assert_eq!(ret1, true);

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
        assert_eq!(ret1, true);

        let delete_ret = bt.delete("ret1".as_bytes().to_vec());
        assert_eq!(delete_ret, true);

        let pos1 = bt.get("ret1".as_bytes().to_vec());
        assert!(pos1.is_none());
    }

    #[test]
    fn test_btree_delete_non_exist_key() {
        let bt = BTree::new();

        let delete_ret = bt.delete("ret1".as_bytes().to_vec());
        assert_eq!(delete_ret, false);

        let pos1 = bt.get("ret1".as_bytes().to_vec());
        assert!(pos1.is_none());
    }
}
