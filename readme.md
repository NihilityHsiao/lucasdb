# lucasdb
lucasdb是一个基于bitcask的kv存储引擎,用于练习rust开发

# 设计
每个 key-value 会当作一个 LogRecord 编码后追加写入到文件中
在内存中维护每个 key-value 在磁盘/文件中的位置(索引)

LogRecord 的编码格式如下：

| type | key size     | value size   | key | value | crc 校验值 |
| ---- | ------------ | ------------ | --- | ----- | ------- |
| 1 字节 | 变长 (最大 5 字节) | 变长 (最大 5 字节) | 变长  | 变长    | 4 字节    |


# 特点
- **快速写入**: 直接采用追加写入日志的方式,写入过程中减少了磁盘磁头的移动。
- **支持批量操作**: 批处理的写入操作会缓存在内存中, 批处理成功提交后才会写入磁盘,失败的批处理数据会被丢弃。
- **支持多种内存索引**: 底层支持 SkipList 和 BTree 作为 key-value 的内存索引。
- **能够处理大于内存的数据**: value 都存放在磁盘中,内存只存放 key 以及位置索引信息。
- **崩溃恢复快速**: lucasdb的数据文件都是追加写入,启动时候会校验数据,确保数据一致。
- **备份简单**: 只需要将数据文件拷贝到任意目录,即可备份整个数据库


# Gettings Started
## 基本操作
```rust
use bytes::Bytes;
use lucasdb::{
    db,
    errors::Errors,
    options::{EngineOptions, IndexType},
};

fn main() {
    // let opts = EngineOptions::default();
    let opts = EngineOptions::builder()
        .dir_path("./tmp/examples".into())
        .data_file_size(256 * 1024 * 1024)
        .sync_writes(false)
        .index_type(IndexType::BTree)
        .bytes_per_sync(0)
        .use_mmap_when_startup(true)
        .build();

    let engine = db::Engine::open(opts).expect("failed to open bitcask engine");

    // put
    let res = engine.put(Bytes::from("hello"), Bytes::from("lucasdb"));
    assert!(res.is_ok());

    // get
    let res = engine.get(Bytes::from("hello"));
    assert!(res.is_ok());
    let value = res.unwrap();
    assert_eq!(value, Bytes::from("lucasdb"));

    // delete
    let res = engine.delete(Bytes::from("hello"));
    assert!(res.is_ok());

    // try to get a non-exist key
    let res = engine.get(Bytes::from("hello"));
    match res {
        Ok(_) => panic!("Expected an error, but got a value"),
        Err(e) => match e {
            Errors::KeyNotFound => {}
            _ => panic!("Unexpected error: {:?}", e),
        },
    }
}
```

## 批量操作
```rust
use bytes::Bytes;
use lucasdb::{
    db::Engine,
    options::{EngineOptions, IndexType, WriteBatchOptions},
};

fn main() {
    let db_opts = EngineOptions::builder()
        .dir_path("./tmp/examples".into())
        .data_file_size(256 * 1024 * 1024)
        .sync_writes(false)
        .index_type(IndexType::BTree)
        .bytes_per_sync(0)
        .use_mmap_when_startup(true)
        .build();

    let wb_opts = WriteBatchOptions::default();

    // 打开一个数据库
    let db = Engine::open(db_opts.clone()).expect("failed to open database");
    // 创建一个批处理
    let wb = db
        .new_write_batch(wb_opts.clone())
        .expect("failed to create write batch");

    // 批量写入
    let _ = wb.put(Bytes::from("key-1"), Bytes::from("value-1"));
    let _ = wb.put(Bytes::from("key-2"), Bytes::from("value-2"));
    let _ = wb.put(Bytes::from("key-3"), Bytes::from("value-3"));
    // 提交数据
    wb.commit().expect("failed to commit write batch");

    // 关闭
    db.close().expect("failed to close database");

    // 打开一个新的db实例
    let db = Engine::open(db_opts.clone()).expect("failed to open database 2");

    let keys = db.list_keys().unwrap();
    for key in keys.iter() {
        let key = key.to_vec();
        println!("{}", String::from_utf8(key).unwrap());
    }
}
```


# benches
```bash
cargo bench
```