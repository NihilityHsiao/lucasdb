use crate::{
    types::{RedisDataType, RedisLucasDb},
    EncodeAndDecode,
};
use bytes::{BufMut, Bytes, BytesMut};
use lucasdb::{
    errors::{Errors, Result},
    options::WriteBatchOptions,
};

pub(crate) struct SetInternalKey {
    pub(crate) key: Vec<u8>,
    pub(crate) version: u128,
    pub(crate) member: Vec<u8>,
}

impl EncodeAndDecode for SetInternalKey {
    /// 编码格式: key + version + member + member.len()
    fn encode(&self) -> bytes::Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&self.key);
        buf.put_u128(self.version);
        buf.extend_from_slice(&self.member);
        buf.put_u32(self.member.len() as u32);
        buf.into()
    }

    fn decode(buf: &mut bytes::Bytes) -> Self {
        todo!()
    }
}

impl RedisLucasDb {
    /// 往`set`添加一个成员\
    /// 添加成功返回true\
    /// 添加失败/member已存在则返回true
    pub fn sadd(&self, key: &str, member: &str) -> Result<bool> {
        let mut meta = self.find_or_new_metadata(key, RedisDataType::Set)?;

        let internal_key = SetInternalKey {
            key: key.as_bytes().to_vec(),
            version: meta.version,
            member: member.as_bytes().to_vec(),
        };

        if let Err(e) = self.eng.get(internal_key.encode()) {
            match e {
                lucasdb::errors::Errors::KeyNotFound => {
                    // 更新元数据
                    let wb = self.eng.new_write_batch(WriteBatchOptions::default())?;
                    meta.size += 1; // 增加了一个member
                    wb.put(Bytes::copy_from_slice(key.as_bytes()), meta.encode())?;

                    // 数据部分,value不用存放
                    wb.put(internal_key.encode(), Bytes::new())?;
                    wb.commit()?;
                    return Ok(true);
                }
                _ => {}
            }
        }

        Ok(false)
    }

    /// 判断member是否在集合中
    pub fn sismember(&self, key: &str, member: &str) -> Result<bool> {
        let meta = self.find_or_new_metadata(key, RedisDataType::Set)?;

        if meta.size == 0 {
            return Ok(false);
        }

        let internal_key = SetInternalKey {
            key: key.as_bytes().to_vec(),
            version: meta.version,
            member: member.as_bytes().to_vec(),
        };

        match self.eng.get(internal_key.encode()) {
            Ok(_) => return Ok(true),
            Err(e) => match e {
                Errors::KeyNotFound => {
                    return Ok(false);
                }
                _ => return Err(e),
            },
        }
    }

    /// 将member从set中删除\
    /// 若member不属于set,返回false
    pub fn srem(&self, key: &str, member: &str) -> Result<bool> {
        let mut meta = self.find_or_new_metadata(key, RedisDataType::Set)?;

        if meta.size == 0 {
            return Ok(false);
        }

        let internal_key = SetInternalKey {
            key: key.as_bytes().to_vec(),
            version: meta.version,
            member: member.as_bytes().to_vec(),
        };

        if let Ok(_) = self.eng.get(internal_key.encode()) {
            // 更新元数据
            meta.size -= 1;
            let wb = self.eng.new_write_batch(WriteBatchOptions::default())?;
            wb.put(Bytes::copy_from_slice(key.as_bytes()), meta.encode())?;
            wb.delete(internal_key.encode())?;
            wb.commit()?;
            return Ok(true);
        }

        return Ok(false);
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
    fn test_set_sadd() {
        let name = "sadd";
        let (rds, _) = setup(name);

        // 添加不存在的成员
        {
            let res = rds.sadd("lucas-set", "val-1");
            assert_eq!(res.ok().unwrap(), true);

            let res = rds.sadd("lucas-set", "val-2");
            assert_eq!(res.ok().unwrap(), true);

            let res = rds.sadd("lucas-set", "val-3");
            assert_eq!(res.ok().unwrap(), true);

            let res = rds.sadd("lucas-set", "val-4");
            assert_eq!(res.ok().unwrap(), true);
        }

        // 添加已存在的成员(重复添加)
        {
            let res = rds.sadd("lucas-set", "val-1");
            assert_eq!(res.ok().unwrap(), false);

            let res = rds.sadd("lucas-set", "val-2");
            assert_eq!(res.ok().unwrap(), false);

            let res = rds.sadd("lucas-set", "val-3");
            assert_eq!(res.ok().unwrap(), false);

            let res = rds.sadd("lucas-set", "val-4");
            assert_eq!(res.ok().unwrap(), false);
        }

        clean(name);
    }

    #[test]
    fn test_set_sismember() {
        let name = "sismember";
        let (rds, _) = setup(name);

        // 添加成员
        {
            {
                let res = rds.sadd("lucas-set", "val-1");
                assert_eq!(res.ok().unwrap(), true);

                let res = rds.sadd("lucas-set", "val-2");
                assert_eq!(res.ok().unwrap(), true);

                let res = rds.sadd("lucas-set", "val-3");
                assert_eq!(res.ok().unwrap(), true);

                let res = rds.sadd("lucas-set", "val-4");
                assert_eq!(res.ok().unwrap(), true);
            }
        }

        // 判断成员存在
        {
            let res = rds.sismember("lucas-set", "val-1");
            assert_eq!(res.ok().unwrap(), true);

            let res = rds.sismember("lucas-set", "val-2");
            assert_eq!(res.ok().unwrap(), true);

            let res = rds.sismember("lucas-set", "val-3");
            assert_eq!(res.ok().unwrap(), true);

            let res = rds.sismember("lucas-set", "val-4");
            assert_eq!(res.ok().unwrap(), true);
        }

        // 判断成员不存在
        {
            let res = rds.sismember("lucas-set", "val-5");
            assert_eq!(res.ok().unwrap(), false);

            let res = rds.sismember("lucas-set", "val-6");
            assert_eq!(res.ok().unwrap(), false);

            let res = rds.sismember("lucas-set", "val-7");
            assert_eq!(res.ok().unwrap(), false);

            let res = rds.sismember("lucas-set", "val-8");
            assert_eq!(res.ok().unwrap(), false);
        }

        clean(name);
    }

    #[test]
    fn test_set_srem() {
        let name = "srem";
        let (rds, _) = setup(name);

        // 添加成员
        {
            let res = rds.sadd("lucas-set", "val-1");
            assert_eq!(res.ok().unwrap(), true);

            let res = rds.sadd("lucas-set", "val-2");
            assert_eq!(res.ok().unwrap(), true);

            let res = rds.sadd("lucas-set", "val-3");
            assert_eq!(res.ok().unwrap(), true);

            let res = rds.sadd("lucas-set", "val-4");
            assert_eq!(res.ok().unwrap(), true);
        }

        // 删除成员
        {
            let res = rds.srem("lucas-set", "val-1");
            assert_eq!(res.is_ok(), true);

            let res = rds.srem("lucas-set", "val-2");
            assert_eq!(res.ok().unwrap(), true);

            let res = rds.srem("lucas-set", "val-3");
            assert_eq!(res.ok().unwrap(), true);

            let res = rds.srem("lucas-set", "val-4");
            assert_eq!(res.ok().unwrap(), true);
        }

        // 判断成员不存在
        {
            let res = rds.sismember("lucas-set", "val-1");
            assert_eq!(res.ok().unwrap(), false);

            let res = rds.sismember("lucas-set", "val-2");
            assert_eq!(res.ok().unwrap(), false);

            let res = rds.sismember("lucas-set", "val-3");
            assert_eq!(res.ok().unwrap(), false);

            let res = rds.sismember("lucas-set", "val-4");
            assert_eq!(res.ok().unwrap(), false);
        }

        // 删除不存在的成员
        {
            let res = rds.srem("lucas-set", "non-exist-1");
            assert_eq!(res.is_ok(), false);

            let res = rds.srem("lucas-set", "non-exist-2");
            assert_eq!(res.ok().unwrap(), false);

            let res = rds.srem("lucas-set", "non-exist-3");
            assert_eq!(res.ok().unwrap(), false);

            let res = rds.srem("lucas-set", "non-exist-4");
            assert_eq!(res.ok().unwrap(), false);
        }

        clean(name);
    }
}
