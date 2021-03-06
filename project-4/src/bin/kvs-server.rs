#[macro_use]
extern crate clap;

use std::path::Path;
use clap::App;
use kvs::{KvStore, Result};
use kvs::error::KvsError;
use kvs::server::KvsServer;
use kvs::thread_pool::{SharedQueueThreadPool, ThreadPool};

fn main() -> Result<()> {
    kvs::logger::init_logger();
    // The YAML file is found relative to the current file, similar to how modules are found
    let yaml = load_yaml!("cli-server.yml");
    let m = App::from(yaml)
        .version(crate_version!())
        .get_matches();

    let address = m.value_of("addr").unwrap_or("127.0.0.1:4000");
    log::info!("address={}", address);
    let engine_name = m.value_of("engine").unwrap_or("kvs");
    log::info!("engine_name={}", engine_name);
    log::info!("version={}", crate_version!());

    let engine_lock_path = Path::new("engine.lock");
    if Path::exists(engine_lock_path) {
        let existed_engine = std::fs::read_to_string(engine_lock_path)?;
        if existed_engine != engine_name {
            Err(KvsError::WrongEngine { expect: existed_engine, actual: engine_name.to_owned() })?
        }
    } else {
        std::fs::write(engine_lock_path, engine_name)?;
    }

    let open_path = "./db";
    if !Path::exists(open_path.as_ref()) {
        std::fs::create_dir_all(open_path)?;
    }

    if !["kvs", "sled"].contains(&engine_name){
        panic!("unsupported engine name")
    }

    let engine = KvStore::open(open_path)?;
    let thread_pool = SharedQueueThreadPool::new(num_cpus::get() as u32)?;
    let mut server = KvsServer::new(address.to_owned(), engine, thread_pool);
    server.start()?;

    Ok(())
}
