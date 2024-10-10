use bytes::{BufMut, Bytes, BytesMut};
use lucasdb::{errors::Result, options::WriteBatchOptions};

use crate::{
    types::{RedisDataType, RedisLucasDb},
    EncodeAndDecode,
};

pub(crate) struct ListInternalKey {
    pub(crate) key: Vec<u8>,
    pub(crate) version: u128,
    pub(crate) index: u64,
}

impl EncodeAndDecode for ListInternalKey {
    fn encode(&self) -> bytes::Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&self.key);
        buf.put_u128(self.version);
        buf.put_u64(self.index);
        buf.into()
    }

    fn decode(buf: &mut bytes::Bytes) -> Self {
        todo!()
    }
}

impl RedisLucasDb {
    /// 从list前面push一个element,返回key下有多少个数据
    pub fn lpush(&self, key: &str, element: &str) -> Result<u32> {
        self.inner_push(key, element, true)
    }

    pub fn rpush(&self, key: &str, element: &str) -> Result<u32> {
        self.inner_push(key, element, false)
    }

    pub fn lpop(&self, key: &str) -> Result<Option<String>> {
        self.inner_pop(key, true)
    }

    pub fn rpop(&self, key: &str) -> Result<Option<String>> {
        self.inner_pop(key, false)
    }

    pub fn inner_push(&self, key: &str, element: &str, is_left_push: bool) -> Result<u32> {
        let mut meta = self.find_or_new_metadata(key, RedisDataType::List)?;

        let internal_key = ListInternalKey {
            key: key.as_bytes().to_vec(),
            version: meta.version,
            index: match is_left_push {
                true => meta.head - 1,
                false => meta.tail,
            },
        };

        // 更新元数据
        meta.size += 1;
        if is_left_push {
            meta.head -= 1;
        } else {
            meta.tail += 1;
        }
        let wb = self.eng.new_write_batch(WriteBatchOptions::default())?;
        wb.put(Bytes::copy_from_slice(key.as_bytes()), meta.encode())?;
        wb.put(
            internal_key.encode(),
            Bytes::copy_from_slice(element.as_bytes()),
        )?;
        wb.commit()?;

        Ok(meta.size)
    }

    pub fn inner_pop(&self, key: &str, is_left_pop: bool) -> Result<Option<String>> {
        let mut meta = self.find_or_new_metadata(key, RedisDataType::List)?;

        if meta.size == 0 {
            return Ok(None);
        }

        let internal_key = ListInternalKey {
            key: key.as_bytes().to_vec(),
            version: meta.version,
            index: match is_left_pop {
                true => meta.head,
                false => meta.tail - 1,
            },
        };

        let element = self.eng.get(internal_key.encode())?;

        // 更新元数据
        meta.size -= 1;
        if is_left_pop {
            meta.head += 1;
        } else {
            meta.tail -= 1;
        }

        {
            let wb = self.eng.new_write_batch(WriteBatchOptions::default())?;
            wb.put(Bytes::copy_from_slice(key.as_bytes()), meta.encode())?;
            wb.delete(internal_key.encode())?;
            wb.commit()?;
        }

        Ok(Some(String::from_utf8(element.to_vec())?))
    }
}

#[cfg(test)]
mod tests {

    use std::path::PathBuf;

    use lucasdb::options::EngineOptions;

    use super::*;

    fn basepath() -> PathBuf {
        "../tmp/redis_lucasdb".into()
    }

    fn setup(name: &str) -> (RedisLucasDb, EngineOptions) {
        clean(name);
        // 创建测试文件夹
        let path = PathBuf::from(basepath()).join(name);
        if !path.exists() {
            match std::fs::create_dir_all(&path) {
                Ok(_) => {}
                Err(e) => {
                    panic!("error creating directory: {}", e)
                }
            }
        }

        let mut opts = EngineOptions::default();
        opts.dir_path = path;
        let redis = RedisLucasDb::new(opts.clone()).expect("failed to create database");
        (redis, opts)
    }

    fn clean(name: &str) {
        let _ = std::fs::remove_dir_all(basepath().join(name));
    }

    #[test]
    fn test_list_lpush() {
        let name = "lpush";
        let (db, _) = setup(name);

        let res = db.lpush("key", "element-1");
        assert_eq!(res.ok().unwrap(), 1);

        let res = db.lpush("key", "element-2");
        assert_eq!(res.ok().unwrap(), 2);

        let res = db.lpush("key", "element-3");
        assert_eq!(res.ok().unwrap(), 3);
        clean(name);
    }

    #[test]
    fn test_list_rpush() {
        let name = "rpush";
        let (db, _) = setup(name);

        let res = db.rpush("key", "element-1");
        assert_eq!(res.ok().unwrap(), 1);

        let res = db.rpush("key", "element-2");
        assert_eq!(res.ok().unwrap(), 2);

        let res = db.rpush("key", "element-3");
        assert_eq!(res.ok().unwrap(), 3);
        clean(name);
    }

    #[test]
    fn test_list_lpop() {
        let name = "lpop";
        let (db, _) = setup(name);

        // 准备数据
        {
            let res = db.lpush("key", "element-1");
            assert_eq!(res.ok().unwrap(), 1);

            let res = db.lpush("key", "element-2");
            assert_eq!(res.ok().unwrap(), 2);

            let res = db.lpush("key", "element-3");
            assert_eq!(res.ok().unwrap(), 3);
        }

        // pop
        {
            let pop_res = db.lpop("key");
            assert!(pop_res.is_ok());
            let pop_res = pop_res.unwrap();
            assert_eq!(pop_res.unwrap(), "element-3");

            let pop_res = db.lpop("key");
            assert!(pop_res.is_ok());
            let pop_res = pop_res.unwrap();
            assert_eq!(pop_res.unwrap(), "element-2");

            let pop_res = db.lpop("key");
            assert!(pop_res.is_ok());
            let pop_res = pop_res.unwrap();
            assert_eq!(pop_res.unwrap(), "element-1");
        }

        // pop(empty key)
        {
            let pop_res = db.lpop("key");
            assert!(pop_res.is_ok());
            let pop_res = pop_res.unwrap();
            assert!(pop_res.is_none());
        }

        clean(name);
    }

    #[test]
    fn test_list_rpop() {
        let name = "rpop";
        let (db, _) = setup(name);
        // 准备数据 [left] 3 - 2 - 1 [right]
        {
            let res = db.lpush("key", "element-1");
            assert_eq!(res.ok().unwrap(), 1);

            let res = db.lpush("key", "element-2");
            assert_eq!(res.ok().unwrap(), 2);

            let res = db.lpush("key", "element-3");
            assert_eq!(res.ok().unwrap(), 3);
        }

        // pop
        {
            let pop_res = db.rpop("key");
            assert!(pop_res.is_ok());
            let pop_res = pop_res.unwrap();
            assert_eq!(pop_res.unwrap(), "element-1");

            let pop_res = db.rpop("key");
            assert!(pop_res.is_ok());
            let pop_res = pop_res.unwrap();
            assert_eq!(pop_res.unwrap(), "element-2");

            let pop_res = db.rpop("key");
            assert!(pop_res.is_ok());
            let pop_res = pop_res.unwrap();
            assert_eq!(pop_res.unwrap(), "element-3");
        }

        // pop(empty key)
        {
            let pop_res = db.lpop("key");
            assert!(pop_res.is_ok());
            let pop_res = pop_res.unwrap();
            assert!(pop_res.is_none());
        }
        clean(name);
    }
}
