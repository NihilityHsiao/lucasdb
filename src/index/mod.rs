use crate::prelude::*;
pub mod btree;
pub mod btree_iterator;
pub mod skiplist;
pub mod skiplist_iterator;

use bytes::Bytes;

use crate::{
    data::log_record::LogRecordPos,
    options::{IndexType, IteratorOptions},
};

/// 内存索引抽象接口
pub trait Indexer: Sync + Send {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool;
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;
    fn delete(&self, key: Vec<u8>) -> bool;
    /// 返回索引迭代器
    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator>;
    /// 获取所有 key
    fn list_keys(&self) -> Result<Vec<Bytes>>;
}

pub trait IndexIterator: Sync + Send {
    /// 回到迭代器的起点,指向第一个数据
    fn rewind(&mut self);

    /// 根据传入的key找到第一个 大于/等于 或 小于/等于 的目标key, 从这个key开始遍历
    fn seek(&mut self, key: Vec<u8>);

    /// 移动到下一个 key, 返回 None 说明迭代完毕
    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)>;
}

pub fn new_indexer(index_type: IndexType) -> impl Indexer {
    match index_type {
        IndexType::BTree => btree::BTree::new(),
        IndexType::SkipList => todo!(),
    }
}
