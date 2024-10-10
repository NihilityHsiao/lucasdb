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
