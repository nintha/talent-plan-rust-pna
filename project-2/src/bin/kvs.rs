#[macro_use]
extern crate clap;

use clap::App;
use kvs::{KvStore, Result};
use std::process::exit;

fn main() -> Result<()> {
    // The YAML file is found relative to the current file, similar to how modules are found
    let yaml = load_yaml!("cli.yml");
    let m = App::from(yaml)
        .name(crate_name!())
        .version(crate_version!())
        .get_matches();

    let mut store = KvStore::open("./")?;

    match m.subcommand() {
        ("get", Some(sub)) => {
            let key = sub.value_of("KEY").unwrap_or("").to_owned();
            if let Some(v) = store.get(key)? {
                println!("{}", v);
            } else {
                println!("Key not found");
            }
        }
        ("set", Some(sub)) => {
            store.set(
                sub.value_of("KEY").unwrap_or("").to_owned(),
                sub.value_of("VALUE").unwrap_or("").to_owned(),
            )?;
        }
        ("rm", Some(sub)) => {
            let key = sub.value_of("KEY").unwrap_or("").to_owned();
            if let Err(e) = store.remove(key) {
                println!("{}", e.to_string());
                exit(1);
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}
