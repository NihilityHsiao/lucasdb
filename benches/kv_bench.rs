use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use lucasdb::db::Engine;
use rand::Rng;
use std::{hint::black_box, path::PathBuf};
#[allow(dead_code)]
pub fn get_test_kv(i: usize) -> (Bytes, Bytes) {
    (
        Bytes::from(format!("lucasdb-key-{:09}", i)),
        Bytes::from(format!("lucasdb-{:1009}-value-value-value-value-value-value-value-value-value-value-value-value-value-value-value", i)),
    )
}

fn benchmark_put(c: &mut Criterion) {
    // 打开存储引擎
    let mut options = lucasdb::options::EngineOptions::default();
    options.dir_path = PathBuf::from("./tmp/benches");
    let engine = Engine::open(options).expect("failed to open engine");

    let mut rnd: rand::rngs::ThreadRng = rand::thread_rng();

    c.bench_function("lucasdb-put-bench", |b| {
        b.iter(|| {
            // 基准测试的逻辑
            let i = rnd.gen_range(0..std::usize::MAX);

            let (k, v) = get_test_kv(i);
            let res = engine.put(k, v);
            assert!(res.is_ok());
        });
    });
}

fn benchmark_get(c: &mut Criterion) {
    // 打开存储引擎
    let mut options = lucasdb::options::EngineOptions::default();
    options.dir_path = PathBuf::from("./tmp/benches");
    let engine = Engine::open(options).expect("failed to open engine");

    let mut rnd: rand::rngs::ThreadRng = rand::thread_rng();

    // 写入数据
    for i in 0..10000 {
        let (k, v) = get_test_kv(i);
        let res = engine.put(k, v);
        assert!(res.is_ok());
    }

    c.bench_function("lucasdb-get-bench", |b| {
        b.iter(|| {
            // 基准测试的逻辑
            let i = rnd.gen_range(0..10000);

            let (k, _) = get_test_kv(i);
            let res = engine.get(k);
            assert!(res.is_ok());
        });
    });
}

fn benchmark_delete(c: &mut Criterion) {
    // 打开存储引擎
    let mut options = lucasdb::options::EngineOptions::default();
    options.dir_path = PathBuf::from("./tmp/benches");
    let engine = Engine::open(options).expect("failed to open engine");

    let mut rnd: rand::rngs::ThreadRng = rand::thread_rng();

    c.bench_function("lucasdb-delete-bench", |b| {
        b.iter(|| {
            // 基准测试的逻辑
            let i = rnd.gen_range(0..std::usize::MAX);

            let (k, v) = get_test_kv(i);
            let res = engine.get(k);
            match res {
                Ok(value) => assert_eq!(value, v),
                Err(_) => {}
            }
        });
    });
}

criterion_group!(benches, benchmark_put, benchmark_get, benchmark_delete);
criterion_main!(benches);
