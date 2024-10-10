use crate::{
    types::{RedisDataType, RedisLucasDb},
    EncodeAndDecode,
};
use bytes::{BufMut, Bytes, BytesMut};
use lucasdb::{
    errors::{Errors, Result},
    options::WriteBatchOptions,
};

pub(crate) struct ZSetInternalKey {
    pub(crate) key: Vec<u8>,
    pub(crate) version: u128,
    pub(crate) score: f64,
    pub(crate) member: Vec<u8>,
}

impl ZSetInternalKey {
    /// 用来根据key+memer拿到score
    fn encode_member(&self) -> bytes::Bytes {
        let mut buf = BytesMut::new();

        buf.extend_from_slice(&self.key);
        buf.put_u128(self.version);
        buf.extend_from_slice(&self.member);

        buf.into()
    }

    /// 用于将member按照score进行排序
    fn encode_score(&self) -> bytes::Bytes {
        let mut buf = BytesMut::new();

        buf.extend_from_slice(&self.key);
        buf.put_u128(self.version);
        // buf.put_f64(self.score);
        buf.extend_from_slice(&self.score.to_string().as_bytes());
        buf.extend_from_slice(&self.member);
        buf.put_u32(self.member.len() as u32);

        buf.into()
    }
}

impl RedisLucasDb {
    /// 不支持负数score
    /// 如果member已经存在,只更新score,返回false
    pub fn zadd(&self, key: &str, score: f64, member: &str) -> Result<bool> {
        let mut meta = self.find_or_new_metadata(key, RedisDataType::ZSet)?;
        let internal_key = ZSetInternalKey {
            key: key.as_bytes().to_vec(),
            version: meta.version,
            score,
            member: member.as_bytes().to_vec(),
        };

        let mut exist = true;
        let mut old_score = 0.0;

        match self.eng.get(internal_key.encode_member()) {
            Ok(val) => {
                let val = String::from_utf8(val.to_vec())?;
                old_score = val.parse().unwrap();
                if old_score == score {
                    return Ok(false);
                }
            }
            Err(e) => match e {
                Errors::KeyNotFound => {
                    exist = false;
                }
                _ => return Err(e),
            },
        }

        // 更新元数据
        let wb = self.eng.new_write_batch(WriteBatchOptions::default())?;
        if !exist {
            meta.size += 1;
            wb.put(Bytes::copy_from_slice(key.as_bytes()), meta.encode())?;
        } else {
            // 删掉旧的
            let old_internal_key = ZSetInternalKey {
                key: key.as_bytes().to_vec(),
                version: meta.version,
                score: old_score,
                member: member.as_bytes().to_vec(),
            };
            wb.delete(old_internal_key.encode_score())?;
        }

        // 写入新数据
        wb.put(internal_key.encode_member(), Bytes::from(score.to_string()))?;
        wb.put(internal_key.encode_score(), Bytes::new())?; // 对score进行编码
        wb.commit()?;

        Ok(!exist)
    }

    /// 返回key-member的score
    pub fn zscore(&self, key: &str, member: &str) -> Result<f64> {
        let meta = self.find_or_new_metadata(key, RedisDataType::ZSet)?;
        if meta.size == 0 {
            return Ok(-1 as f64);
        }

        let internal_key = ZSetInternalKey {
            key: key.as_bytes().to_vec(),
            version: meta.version,
            score: 0f64,
            member: member.as_bytes().to_vec(),
        };

        let score_bytes = self.eng.get(internal_key.encode_member())?;
        let score_str = String::from_utf8(score_bytes.to_vec())?;
        let score = score_str.parse().unwrap();
        Ok(score)
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
    fn test_zset_zadd() {
        let name = "zadd";
        let (db, _) = setup(name);

        let res = db.zadd("key", 12f64, "val-1");
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), true);

        // 修改分数
        let res = db.zadd("key", 34f64, "val-1");
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), false);

        // 没修改分数，重复添加
        let res = db.zadd("key", 34f64, "val-1");
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), false);

        clean(name);
    }

    #[test]
    fn test_zset_zscore() {
        let name = "zadd";
        let (db, _) = setup(name);

        {

            
        let res = db.zadd("key", 12f64, "member-1");
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), true);

        let res = db.zadd("key", 520f64, "member-2");
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), true);
        }



        // 获取分数
        {
            let res = db.zscore("key", "member-1");
            assert!(res.is_ok());
            assert_eq!(res.unwrap(),12f64);


            let res = db.zscore("key", "member-2");
            assert!(res.is_ok());
            assert_eq!(res.unwrap(),520f64);
        }

        clean(name);
    }
}
