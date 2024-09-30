use crate::{data::log_record::LogRecordPos, options::IteratorOptions};

use super::IndexIterator;

pub struct BTreeIterator {
    pub(crate) items: Vec<(Vec<u8>, LogRecordPos)>, // 存储 key, 索引
    pub(crate) curr_index: usize,                   // 当前遍历的位置
    pub(crate) options: IteratorOptions,
}

impl IndexIterator for BTreeIterator {
    fn rewind(&mut self) {
        self.curr_index = 0;
    }

    fn seek(&mut self, key: Vec<u8>) {
        self.curr_index = match self.items.binary_search_by(|(x, _)| {
            if self.options.reverse {
                x.cmp(&key).reverse()
            } else {
                x.cmp(&key)
            }
        }) {
            Ok(val) => val,
            Err(insert_val) => insert_val,
        }
    }

    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)> {
        if self.curr_index >= self.items.len() {
            return None;
        }

        while let Some(item) = self.items.get(self.curr_index) {
            self.curr_index += 1;
            let prefix = &self.options.prefix;

            if prefix.is_empty() || item.0.starts_with(&prefix) {
                return Some((&item.0, &item.1));
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use crate::index::{btree::BTree, Indexer};

    use super::*;

    #[test]
    fn test_btree_iterator_seek() {
        // 没有数据的情况
        {
            let bt = BTree::new();
            let mut iter = bt.iterator(IteratorOptions::default());
            let key = "abc".as_bytes().to_vec();

            iter.seek(key.clone());
            let res = iter.next();
            assert!(res.is_none());
        }

        // 有1条数据
        {
            let bt = BTree::new();
            let key = "abc".as_bytes().to_vec();
            let pos = LogRecordPos {
                file_id: 0,
                offset: 10,
            };
            bt.put(key.clone(), pos.clone());

            let mut iter = bt.iterator(IteratorOptions::default());

            iter.seek(key.clone());
            let res = iter.next();
            assert!(res.is_some());

            println!("res: {:?}", res);
        }
    }

    #[test]
    fn test_btree_iterator_seek_with_prefix() {
        let bt = BTree::new();
        let prefix = "aa";
        let opts = IteratorOptions::builder()
            .prefix(prefix.as_bytes().to_vec())
            .reverse(false)
            .build();

        // 放入多条数据
        {
            let key = "aa-11-22".as_bytes().to_vec();
            let pos = LogRecordPos {
                file_id: 0,
                offset: 10,
            };
            bt.put(key.clone(), pos.clone());
            let key = "aa-33-44".as_bytes().to_vec();
            let pos = LogRecordPos {
                file_id: 0,
                offset: 10,
            };
            bt.put(key.clone(), pos.clone());
            let key = "bb-11-22".as_bytes().to_vec();
            let pos = LogRecordPos {
                file_id: 0,
                offset: 10,
            };
            bt.put(key.clone(), pos.clone());
            let key: Vec<u8> = "bb-33-44".as_bytes().to_vec();
            let pos = LogRecordPos {
                file_id: 0,
                offset: 10,
            };
            bt.put(key.clone(), pos.clone());
        }

        // 查找,应该只包含aa开头的key
        {
            let mut iter = bt.iterator(opts);
            while let Some((key, _)) = iter.next() {
                let key_str = String::from_utf8(key.clone());
                assert!(key_str.is_ok());
                let key_str = key_str.unwrap();

                assert!(key_str.starts_with(prefix));
            }
        }
    }

    #[test]
    fn test_btree_iterator_next() {}
}
