use bytes::Bytes;

pub mod generic;
pub mod hash;
pub(crate) mod metadata;
pub mod set;
pub mod string;
pub mod types;
pub(crate) trait EncodeAndDecode {
    fn encode(&self) -> Bytes;
    fn decode(buf: &mut Bytes) -> Self;
}
