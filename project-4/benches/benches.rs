use criterion::{Criterion, criterion_group, criterion_main, ParameterizedBenchmark, BatchSize};
use tempfile::TempDir;
use kvs::{KvStore, KvsEngine};
use std::iter;

fn set_bench(c: &mut Criterion) {
    let bench = ParameterizedBenchmark::new(
        "kvs",
        |b, _| {
            b.iter_batched(
                || {
                    let temp_dir = TempDir::new().unwrap();
                    (KvStore::open(temp_dir.path()).unwrap(), temp_dir)
                },
                |(store, _temp_dir)| {
                    for i in 1..(1 << 12) {
                        store.set(format!("key{}", i), "value".to_string()).unwrap();
                    }
                },
                BatchSize::SmallInput,
            )
        },
        iter::once(()),
    );
    c.bench("set_bench", bench);
}

criterion_group!(benches, set_bench);
criterion_main!(benches);