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
