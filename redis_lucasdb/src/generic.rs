use crate::types::{RedisDataType, RedisLucasDb};
use bytes::{Buf, Bytes};
use lucasdb::errors::Result;
impl RedisLucasDb {
    pub fn del(&self, key: &str) -> Result<()> {
        let ret = self.eng.delete(Bytes::copy_from_slice(key.as_bytes()));
        ret
    }

    /// 返回`key`的类型
    pub fn key_type(&self, key: &str) -> Result<RedisDataType> {
        let mut buf = self.eng.get(Bytes::copy_from_slice(key.as_bytes()))?;
        Ok(RedisDataType::from(buf.get_u8()))
    }
}
