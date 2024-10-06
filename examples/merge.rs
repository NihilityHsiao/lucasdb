use bytes::Bytes;
use lucasdb::{
    db::Engine,
    options::{EngineOptions, IndexType, WriteBatchOptions},
};
fn get_test_kv(i: usize) -> (Bytes, Bytes) {
    let key = Bytes::copy_from_slice(format!("test_lucas_db_key_{:09}", i).as_bytes());
    let value = Bytes::copy_from_slice(format!("test_lucas_db_value_{:09}", i).as_bytes());

    (key, value)
}
fn main() {
    let opts = EngineOptions::builder()
        .dir_path("./tmp/examples".into())
        .data_file_size(256 * 1024 * 1024)
        .sync_writes(false)
        .index_type(IndexType::BTree)
        .bytes_per_sync(0)
        .use_mmap_when_startup(true)
        .data_file_merge_ratio(0f32)
        .build();

    let db = Engine::open(opts.clone()).expect("failed to open database");

    // 写入数据
    let begin = 0;
    let end = 50000;
    {
        for i in begin..end {
            let (key, value) = get_test_kv(i);
            let put_res = db.put(key, value);
            assert!(put_res.is_ok());
        }
    }

    // 第一次 merge
    {
        let merge_res = db.merge();
        match merge_res {
            Ok(_) => {}
            Err(e) => panic!("merge failed: {}", e),
        }
    }

    // 关闭db
    {
        println!("drop db begin");
        std::mem::drop(db);
        println!("drop db end");
    }

    // 重新打开db
    println!("open db again begin");

    let db = match Engine::open(opts.clone()) {
        Ok(db) => db,
        Err(e) => {
            panic!("{}", format!("open db error: {}", e));
        }
    };

    println!("open db again end");
    // 重新校验
    {
        let keys = db.list_keys().unwrap();
        assert_eq!(keys.len(), end - begin);
    }
}
