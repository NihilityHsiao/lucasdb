use bytes::Bytes;
use lucasdb::{
    db::Engine,
    options::{EngineOptions, IndexType, IteratorOptions},
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
    let end = 10;
    {
        for i in begin..end {
            let (key, value) = get_test_kv(i);
            let put_res = db.put(key, value);
            assert!(put_res.is_ok());
        }
    }

    // 迭代器遍历
    let it = db.iter(IteratorOptions::default());
    while let Some((key, value)) = it.next() {
        let key = String::from_utf8(key.to_vec()).unwrap();
        let value = String::from_utf8(value.to_vec()).unwrap();
        println!("key: {:?},  value:{:?}", key, value);
    }
}
