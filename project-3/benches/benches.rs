use criterion::{Criterion, criterion_group, criterion_main, ParameterizedBenchmark, BatchSize, BenchmarkId};
use tempfile::TempDir;
use kvs::{KvStore, KvsEngine};
use rand::prelude::*;
use kvs::engines::sled::SledKvsEngine;
use std::iter;
use rand::distributions::Alphanumeric;

fn gen_rand_string(rng: &mut SmallRng, max_len: usize) -> String {
    let len = rng.gen_range(1, max_len);
    return rng
        .sample_iter(&Alphanumeric)
        .take(len)
        .collect();
}

fn write_bench(c: &mut Criterion) {
    let mut rng = SmallRng::from_seed([0; 16]);

    let mut group = c.benchmark_group("write_bench");
    for engine_name in ["kvs", "sled"].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(engine_name),
            engine_name,
            |b, &engine_name| {
                let temp_dir = TempDir::new().unwrap();
                let mut engine: Box<dyn KvsEngine> = if engine_name == "kvs" {
                    Box::new(KvStore::open(temp_dir.path()).unwrap())
                } else {
                    Box::new(SledKvsEngine::open(temp_dir.path()).unwrap())
                };

                b.iter(|| {
                    for _i in 0..100 {
                        let key = gen_rand_string(&mut rng, 10_000);
                        let value = gen_rand_string(&mut rng, 10_000);
                        engine.set(key, value).unwrap();
                    }
                });
            });
    }
    group.finish();
}


fn set_bench(c: &mut Criterion) {
    let bench = ParameterizedBenchmark::new(
        "kvs",
        |b, _| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    (KvStore::open(temp_dir.path()).unwrap(), temp_dir)
                },
                |(mut store, _temp_dir)| {
                    for i in 1..(1 << 12) {
                        store.set(format!("key{}", i), "value".to_string()).unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        },
        iter::once(()),
    )
        .with_function("sled", |b, _| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    (SledKvsEngine::open(temp_dir.path()).unwrap(), temp_dir)
                },
                |(mut db, _temp_dir)| {
                    for i in 1..(1 << 12) {
                        db.set(format!("key{}", i), "value".to_string()).unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        });
    c.bench("set_bench", bench);
}

fn get_bench(c: &mut Criterion) {
    let bench = ParameterizedBenchmark::new(
        "kvs",
        |b, i| {
            let temp_dir = TempDir::new().unwrap();
            let mut store = KvStore::open(temp_dir.path()).unwrap();
            for key_i in 1..(1 << i) {
                store
                    .set(format!("key{}", key_i), "value".to_string())
                    .unwrap();
            }
            let mut rng = SmallRng::from_seed([0; 16]);
            b.iter(|| {
                store
                    .get(format!("key{}", rng.gen_range(1, 1 << i)))
                    .unwrap();
            });
        },
        vec![8],
    )
        .with_function("sled", |b, i| {
            let temp_dir = TempDir::new().unwrap();
            let mut db = SledKvsEngine::open(&temp_dir.path()).unwrap();
            for key_i in 1..(1 << i) {
                db.set(format!("key{}", key_i), "value".to_string())
                    .unwrap();
            }
            let mut rng = SmallRng::from_seed([0; 16]);
            b.iter(|| {
                db.get(format!("key{}", rng.gen_range(1, 1 << i))).unwrap();
            });
        });
    c.bench("get_bench", bench);
}
criterion_group!(benches, write_bench, get_bench, set_bench);
criterion_main!(benches);