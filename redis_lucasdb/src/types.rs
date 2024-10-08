use core::fmt;

use lucasdb::errors::Result;
use lucasdb::options::EngineOptions;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RedisDataType {
    String,
    Hash,
    Set,
    List,
    ZSet,
}

impl From<u8> for RedisDataType {
    fn from(value: u8) -> Self {
        match value {
            0 => RedisDataType::String,
            1 => RedisDataType::Hash,
            2 => RedisDataType::Set,
            3 => RedisDataType::List,
            4 => RedisDataType::ZSet,
            _ => panic!("Invalid Redis data type"),
        }
    }
}

impl fmt::Display for RedisDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisDataType::String => write!(f, "String"),
            RedisDataType::Hash => write!(f, "Hash"),
            RedisDataType::Set => write!(f, "Set"),
            RedisDataType::List => write!(f, "List"),
            RedisDataType::ZSet => write!(f, "ZSet"),
        }
    }
}

pub struct RedisLucasDb {
    pub(crate) eng: lucasdb::db::Engine,
}

impl RedisLucasDb {
    pub fn new(options: EngineOptions) -> Result<Self> {
        let engine = lucasdb::db::Engine::open(options)?;
        Ok(Self { eng: engine })
    }
}
