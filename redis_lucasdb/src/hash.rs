use std::time::SystemTime;

use crate::{
    metadata::Metadata,
    types::{RedisDataType, RedisLucasDb},
    EncodeAndDecode,
};
use bytes::{BufMut, Bytes, BytesMut};
use lucasdb::{
    errors::{Errors, Result},
    options::WriteBatchOptions,
};

const INITIAL_LIST_MARK: u64 = std::u64::MAX / 2;

pub(crate) struct HashInternalKey {
    pub(crate) key: Vec<u8>,
    pub(crate) version: u128,
    pub(crate) field: Vec<u8>,
}

impl EncodeAndDecode for HashInternalKey {
    fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&self.key);
        buf.put_u128(self.version);
        buf.extend_from_slice(&self.field);
        buf.into()
    }

    fn decode(buf: &mut Bytes) -> Self {
        todo!()
    }
}

impl RedisLucasDb {
    /// 根据 hash 的 key 查找元数据
    fn find_metadata(&self, key: &str, data_type: RedisDataType) -> Result<Metadata> {
        let mut exist = true;
        let mut meta = None;
        match self.eng.get(Bytes::copy_from_slice(key.as_bytes())) {
            Ok(mut meta_buf) => {
                let meta_buf_data_type = RedisDataType::from((&meta_buf[0..1])[0]);
                if data_type != RedisDataType::from(meta_buf_data_type) {
                    return Err(Errors::WrongTypeOperation {
                        expected: data_type.to_string(),
                        actual: meta_buf_data_type.to_string(),
                    });
                }
                let metadata = Metadata::decode(&mut meta_buf);
                meta = Some(metadata);

                // 是否过期
                let now = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos();
                let expire = meta.as_ref().unwrap().expire;
                if expire != 0 && expire <= now {
                    exist = false;
                }
            }
            Err(e) => match e {
                Errors::KeyNotFound => {
                    exist = false;
                }
                _ => return Err(e),
            },
        };

        if !exist {
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos();

            let mut metadata = Metadata {
                data_type,
                expire: 0,
                version: now,
                size: 0,
                head: 0,
                tail: 0,
            };

            if data_type == RedisDataType::List {
                metadata.head = INITIAL_LIST_MARK;
                metadata.tail = INITIAL_LIST_MARK;
            }

            meta = Some(metadata);
        }

        Ok(meta.unwrap())
    }

    pub fn hset(&self, key: &str, field: &str, value: &str) -> Result<bool> {
        // 查询元数据
        let mut meta = self.find_metadata(key, RedisDataType::Hash)?;
        // 构造数据部分的key
        let internal_key = HashInternalKey {
            key: key.as_bytes().to_vec(),
            version: meta.version,
            field: field.as_bytes().to_vec(),
        };

        let mut exist = true;
        if let Err(e) = self.eng.get(internal_key.encode()) {
            match e {
                Errors::KeyNotFound => {
                    exist = false;
                }
                _ => {}
            }
        }

        let wb = self.eng.new_write_batch(WriteBatchOptions::default())?;
        if !exist {
            meta.size += 1;
            wb.put(Bytes::copy_from_slice(key.as_bytes()), meta.encode())?;
        }

        wb.put(
            internal_key.encode(),
            Bytes::copy_from_slice(value.as_bytes()),
        )?;
        wb.commit()?;

        Ok(!exist)
    }

    /// 当key/field不存在,返回 KeyNotFound
    pub fn hget(&self, key: &str, field: &str) -> Result<Option<String>> {
        let meta = self.find_metadata(key, RedisDataType::Hash)?;
        if meta.size == 0 {
            return Ok(None);
        }

        let internal_key = HashInternalKey {
            key: key.as_bytes().to_vec(),
            version: meta.version,
            field: field.as_bytes().to_vec(),
        };

        let value = self.eng.get(internal_key.encode())?;
        let value_string = String::from_utf8(value.to_vec())?;

        Ok(Some(value_string))
    }

    ///
    pub fn hdel(&self, key: &str, field: &str) -> Result<bool> {
        let mut meta = self.find_metadata(key, RedisDataType::Hash)?;
        if meta.size == 0 {
            return Ok(false);
        }

        let internal_key = HashInternalKey {
            key: key.as_bytes().to_vec(),
            version: meta.version,
            field: field.as_bytes().to_vec(),
        };

        let mut exist = true;
        if let Err(e) = self.eng.get(internal_key.encode()) {
            match e {
                Errors::KeyNotFound => {
                    exist = false;
                }
                _ => {}
            }
        }

        if exist {
            let wb = self.eng.new_write_batch(WriteBatchOptions::default())?;
            meta.size -= 1;
            wb.put(Bytes::copy_from_slice(key.as_bytes()), meta.encode())?;
            wb.delete(internal_key.encode())?;
            wb.commit()?;

            return Ok(true);
        }

        Ok(exist)
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
    fn test_hash_hget_exist_key_field() {
        let name = "hget_exist_key_field";
        let (rds, _) = setup(name);

        {
            let key = "lucas_hash_hget";
            let field = "lucas_hash_field";
            let value = "lucas_hash_value";
            let set_res = rds.hset(key, field, value);
            assert!(set_res.is_ok());
            let get_res = rds.hget(key, field);
            assert!(get_res.is_ok());
            let get_res = get_res.unwrap();
            assert!(get_res.is_some());
            let get_value = get_res.unwrap();
            assert_eq!(get_value, value);
        }

        {
            let key = "lucas_hash_hget_1";
            let field = "lucas_hash_field_1";
            let value = "lucas_hash_value_1";
            let set_res = rds.hset(key, field, value);
            assert!(set_res.is_ok());
            let get_res = rds.hget(key, field);
            assert!(get_res.is_ok());
            let get_res = get_res.unwrap();
            assert!(get_res.is_some());
            let get_value = get_res.unwrap();
            assert_eq!(get_value, value);
        }
        clean(name);
    }

    /// hget 不存在的 key, field
    #[test]
    fn test_hash_hget_non_exist_key_non_exist_field() {
        let name = "hget_non_exist_key_non_exist_field";
        let (rds, _) = setup(name);

        {
            let key = "lucas_hash_hget";
            let field = "lucas_hash_field";
            let value = "lucas_hash_value";
            let set_res = rds.hset(key, field, value);
            assert!(set_res.is_ok());
            let get_res = rds.hget(key, "non_exist_field");
            match get_res {
                Ok(impossbile_value) => {
                    panic!("should not get non_exist_field: {:?}", impossbile_value)
                }
                Err(e) => match e {
                    Errors::KeyNotFound => {}
                    _ => panic!("unexpected error"),
                },
            }
        }

        {
            let key = "lucas_hash_hget_1";
            let field = "lucas_hash_field_1";
            let value = "lucas_hash_value_1";
            let set_res = rds.hset(key, field, value);
            assert!(set_res.is_ok());
            let get_res = rds.hget(key, "");
            match get_res {
                Ok(impossbile_value) => {
                    panic!("should not get non_exist_field: {:?}", impossbile_value)
                }
                Err(e) => match e {
                    Errors::KeyNotFound => {}
                    _ => panic!("unexpected error"),
                },
            }
        }

        clean(name);
    }

    #[test]
    fn test_hash_hset() {
        let name = "hset";
        let (rds, _) = setup(name);

        // hset 不存在的值
        {
            let set_res = rds.hset("lucas_hash", "field1", "value1");
            println!("{:?}", set_res);
            assert!(set_res.is_ok());

            let set_res = rds.hset("lucas_hash", "field2", "value2");
            println!("{:?}", set_res);

            assert!(set_res.is_ok());
        }

        // hset 已存在的值
        {
            let set_res = rds.hset("lucas_hash", "field1", "value1");
            println!("{:?}", set_res);
            assert!(set_res.is_ok());

            let set_res = rds.hset("lucas_hash", "field2", "value2");
            println!("{:?}", set_res);

            assert!(set_res.is_ok());
        }

        clean(name);
    }

    #[test]
    fn test_hash_hdel() {
        let name = "hdel";
        let (rds, _) = setup(name);

        // 删除不存在的key
        {
            let del_res = rds.hdel("myhash", "field");
            assert!(del_res.is_ok());
            assert_eq!(del_res.unwrap(), false);
        }

        // 删除已有的key
        {
            let set_res = rds.hset("key", "field", "value");
            assert!(set_res.ok().unwrap());

            let del_res = rds.hdel("key", "field");
            assert!(del_res.ok().unwrap());
        }

        // 删除不存在的field
        {
            let del_res = rds.hdel("key", "non-exist-field");
            match del_res {
                Ok(v) => panic!("should not get ok: {}", v),
                Err(e) => match e {
                    Errors::KeyNotFound => {}
                    _ => panic!("unexpected error: {:?}", e),
                },
            }
        }

        clean(name);
    }
}
