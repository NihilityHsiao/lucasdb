use core::time;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use lucasdb::errors::{Errors, Result};

use crate::types::{RedisDataType, RedisLucasDb};

/// 实现redis中对string的操作:get, set
impl RedisLucasDb {
    /// value会经过编码再进行存储
    /// 编码格式： type + ttl + value(用户传进的value)
    pub fn set(&self, key: &str, ttl: std::time::Duration, value: &str) -> Result<()> {
        if value.len() == 0 {
            return Ok(());
        }

        let mut buf = BytesMut::new();
        buf.put_u8(RedisDataType::String as u8); // 1.type

        let mut expire = 0; // 过期时间,纳秒
        if ttl != time::Duration::ZERO {
            if let Some(v) = SystemTime::now().checked_add(ttl) {
                expire = v.duration_since(UNIX_EPOCH).unwrap().as_nanos();
            }
        }

        buf.put_u128(expire); // 2.ttl

        // 3.value部分
        buf.extend_from_slice(value.as_bytes());

        self.eng
            .put(Bytes::copy_from_slice(key.as_bytes()), buf.into())?;

        Ok(())
    }

    // 拿到的value需要解码
    /// 编码格式： type + ttl + value(用户传进的value)
    pub fn get(&self, key: &str) -> Result<Option<String>> {
        let mut buf = self.eng.get(Bytes::copy_from_slice(key.as_bytes()))?;
        let key_type = RedisDataType::from(buf.get_u8());

        // 判断key的类型能否执行get操作
        if key_type != RedisDataType::String {
            return Err(Errors::WrongTypeOperation {
                expected: RedisDataType::String.to_string(),
                actual: key_type.to_string(),
            });
        }

        // 判断过期时间
        let expire = buf.get_u128();
        if expire > 0 {
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            if expire <= now {
                // 过期了
                return Ok(None);
            }
        }

        // 取出真正的value
        // get_u8和get_u128会移动ptr位置,所以直接to_vec就得到value了
        let value = buf.to_vec();

        Ok(Some(String::from_utf8(value).unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, time::Duration};

    use lucasdb::options::EngineOptions;

    use super::*;

    fn basepath() -> PathBuf {
        "../tmp/redis_lucasdb/string".into()
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
    fn test_string_get_and_set_no_ttl() {
        let name = "get_and_set";
        let (db, _) = setup(name);

        let set_res = db.set("key1", Duration::ZERO, "value1");
        assert!(set_res.is_ok());

        let set_res = db.set("key2", Duration::ZERO, "value2");
        assert!(set_res.is_ok());

        let get_res = db.get("key1");
        assert!(get_res.is_ok());
        let get_option = get_res.unwrap();
        assert!(get_option.is_some());
        let value = get_option.unwrap();
        assert_eq!(value, "value1");

        let get_res = db.get("key2");
        assert!(get_res.is_ok());
        let get_option = get_res.unwrap();
        assert!(get_option.is_some());
        let value = get_option.unwrap();
        assert_eq!(value, "value2");

        clean(name);
    }
}
