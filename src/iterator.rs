use crate::prelude::*;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;

use crate::{db::Engine, index::IndexIterator, options::IteratorOptions};

pub struct Iterator<'a> {
    index_iter: Arc<RwLock<Box<dyn IndexIterator>>>, // 索引迭代器
    engine: &'a Engine,
}

impl Engine {
    pub fn iter(&self, options: IteratorOptions) -> Iterator {
        Iterator {
            index_iter: Arc::new(RwLock::new(self.index.iterator(options))),
            engine: self,
        }
    }

    pub fn list_keys(&self) -> Result<Vec<Bytes>> {
        self.index.list_keys()
    }

    /// 对数据库中的所有数据执行某个参数,函数返回false时终止
    pub fn fold<F>(&self, f: F) -> Result<()>
    where
        Self: Sized,
        F: Fn(Bytes, Bytes) -> bool,
    {
        let iter = self.iter(IteratorOptions::default());
        while let Some((key, value)) = iter.next() {
            if !f(key, value) {
                break;
            }
        }
        Ok(())
    }
}

impl Iterator<'_> {
    /// 回到迭代器的起点,指向第一个数据
    pub fn rewind(&self) {
        let mut index_iter = self.index_iter.write();
        index_iter.rewind();
    }

    /// 根据传入的key找到第一个 大于/等于 或 小于/等于 的目标key, 从这个key开始遍历
    pub fn seek(&self, key: Vec<u8>) {
        let mut index_iter = self.index_iter.write();
        index_iter.seek(key);
    }

    /// 移动到下一个 key, 返回 None 说明迭代完毕
    pub fn next(&self) -> Option<(Bytes, Bytes)> {
        let mut index_iter = self.index_iter.write();
        if let Some(item) = index_iter.next() {
            let value = self
                .engine
                .get_value_by_position(item.1)
                .expect("failed to get value from data file");
            return Some((Bytes::from(item.0.to_vec()), value));
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, path::PathBuf, rc::Rc};

    use crate::options::EngineOptions;

    use super::*;
    fn basepath() -> PathBuf {
        PathBuf::from("./tmp/iterator")
    }

    fn setup() {
        // 创建测试文件夹
        let basepath = PathBuf::from(basepath());
        if basepath.exists() {
            return;
        }

        match std::fs::create_dir_all(basepath) {
            Ok(_) => {}
            Err(e) => {
                panic!("error creating directory: {}", e)
            }
        }
    }

    fn clean() {
        let _ = std::fs::remove_dir_all(basepath());
    }
    #[test]
    fn test_iterator_seek() {
        setup();
        let mut opts = EngineOptions::default();
        opts.dir_path = basepath();
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        // 没有数据
        {
            let key = "aa".as_bytes().to_vec();
            let iter = engine.iter(IteratorOptions::default());
            iter.seek(key.clone());

            assert!(iter.next().is_none());
        }

        // 有1条数据
        {
            let key = Bytes::from("aa");
            let value = Bytes::from("bb");
            let put_res = engine.put(key.clone(), value.clone());
            assert!(put_res.is_ok());

            let iter = engine.iter(IteratorOptions::default());
            iter.seek("a".as_bytes().to_vec());
            let next_kv = iter.next();
            assert!(next_kv.is_some());
            let next_kv = next_kv.unwrap();

            assert_eq!(next_kv.0, key.clone());
            assert_eq!(next_kv.1, value.clone());
        }
        clean();
    }

    #[test]
    fn test_iterator_seek_with_prefix() {
        setup();

        let mut opts = EngineOptions::default();
        let prefix = "a";
        opts.dir_path = basepath();
        let engine = Engine::open(opts.clone()).expect("failed to open engine");
        let mut iter_opts = IteratorOptions::default();
        iter_opts.prefix = prefix.as_bytes().to_vec();

        // 填充数据
        {
            let _ = engine.put(Bytes::from("abc-1"), Bytes::from("v1"));
            let _ = engine.put(Bytes::from("abc-2"), Bytes::from("v2"));
            let _ = engine.put(Bytes::from("abc-3"), Bytes::from("v3"));
            let _ = engine.put(Bytes::from("a-1"), Bytes::from("v1"));
            let _ = engine.put(Bytes::from("a-2"), Bytes::from("v2"));
            let _ = engine.put(Bytes::from("a-3"), Bytes::from("v3"));

            let _ = engine.put(Bytes::from("b-1"), Bytes::from("v1"));
            let _ = engine.put(Bytes::from("b-2"), Bytes::from("v2"));
            let _ = engine.put(Bytes::from("b-3"), Bytes::from("v3"));
            let _ = engine.put(Bytes::from("c-1"), Bytes::from("v1"));
            let _ = engine.put(Bytes::from("c-2"), Bytes::from("v2"));
            let _ = engine.put(Bytes::from("c-3"), Bytes::from("v3"));
            let _ = engine.put(Bytes::from("zacb-1"), Bytes::from("v1"));
            let _ = engine.put(Bytes::from("zacb-2"), Bytes::from("v2"));
            let _ = engine.put(Bytes::from("zacb-3"), Bytes::from("v3"));
        }

        // 检查遍历的每个key都是以a开头的
        {
            let iter = engine.iter(iter_opts);
            while let Some((key, _)) = iter.next() {
                let key = String::from_utf8(key.to_vec());
                assert!(key.is_ok());
                let key = key.unwrap();
                assert_eq!(true, key.starts_with(prefix));
            }
        }

        clean();
    }

    #[test]
    fn test_iterator_list_keys() {
        setup();

        let mut opts = EngineOptions::default();
        opts.dir_path = basepath();
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        // 填充数据
        {
            let _ = engine.put(Bytes::from("abc-1"), Bytes::from("v1"));
            let _ = engine.put(Bytes::from("abc-2"), Bytes::from("v2"));
            let _ = engine.put(Bytes::from("abc-3"), Bytes::from("v3"));
        }

        // 校验
        {
            let keys = engine.list_keys();
            assert_eq!(true, keys.is_ok());
            let keys = keys.unwrap();
            assert_eq!(3, keys.len());
        }

        clean();
    }

    #[test]
    fn test_iterator_fold() {
        setup();
        let mut opts = EngineOptions::default();
        opts.dir_path = basepath();
        let engine = Engine::open(opts.clone()).expect("failed to open engine");

        // 填充数据
        {
            let _ = engine.put(Bytes::from("abc-1"), Bytes::from("v1"));
            let _ = engine.put(Bytes::from("abc-2"), Bytes::from("v2"));
            let _ = engine.put(Bytes::from("abc-3"), Bytes::from("v3"));
        }

        // fold 操作
        let count = Rc::new(RefCell::new(0usize));
        engine
            .fold(|key, value| {
                println!("key: {:?} \t value:{:?}", key, value);
                *count.borrow_mut() += 1;
                return true;
            })
            .unwrap();

        let keys = engine.list_keys().unwrap();

        assert_eq!(*count.borrow(), keys.len());
        clean();
    }
}
