use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{types::RedisDataType, EncodeAndDecode};

/// 元数据会编码作为一个`key`, 编码格式: \
/// type + expire + version + size
#[derive(Debug, Clone, Copy)]
pub(crate) struct Metadata {
    pub(crate) data_type: RedisDataType,
    /// 过期时间
    pub(crate) expire: u128,
    pub(crate) version: u128,
    /// 该key/metadata的数据量
    pub(crate) size: u32,
    /// List结构专用
    pub(crate) head: u64,
    /// List结构专用
    pub(crate) tail: u64,
}

impl EncodeAndDecode for Metadata {
    fn encode(&self) -> Bytes {
        let mut buf = BytesMut::new();
        buf.put_u8(self.data_type as u8);
        buf.put_u128(self.expire);
        buf.put_u128(self.version);
        buf.put_u32(self.size);

        if self.data_type == RedisDataType::List {
            buf.put_u64(self.head);
            buf.put_u64(self.tail);
        }
        buf.into()
    }

    fn decode(buf: &mut Bytes) -> Self {
        let data_type = RedisDataType::from(buf.get_u8());
        let expire = buf.get_u128();
        let version = buf.get_u128();
        let size = buf.get_u32();
        let (head, tail) = match data_type {
            RedisDataType::List => {
                let head = buf.get_u64();
                let tail = buf.get_u64();

                (head, tail)
            }
            _ => (0, 0),
        };

        Metadata {
            data_type,
            expire,
            version,
            size,
            head,
            tail,
        }
    }
}
