#[macro_use]
extern crate clap;

use clap::{App, ArgMatches};
use kvs::Result;
use std::process::exit;
use kvs::error::KvsError;
use kvs::client::KvsClient;
use kvs::model::Msg;

fn main() -> Result<()> {
    kvs::log::init_logger();
    // The YAML file is found relative to the current file, similar to how modules are found
    let yaml = load_yaml!("cli-client.yml");
    let m = App::from(yaml)
        .version(crate_version!())
        .get_matches();

    match m.subcommand() {
        ("get", Some(sub)) => {
            let key = sub.value_of("KEY").unwrap_or("").to_owned();
            let address = get_address_from_args(sub)?;
            let mut client = KvsClient::connect(address)?;
            let req = Msg::build_bulk_array(&vec!["get".to_owned(), key]);
            let res = client.request_msg(req)?;
            match res {
                Msg::Bulk(ops) => {
                    if let Some(v) = ops {
                        println!("{}", v);
                    } else {
                        println!("Key not found");
                    }
                }
                Msg::Error(e) => {
                    eprintln!("{}", e);
                    exit(1);
                }
                _ => unreachable!()
            }
        }
        ("set", Some(sub)) => {
            let key = sub.value_of("KEY").unwrap_or("").to_owned();
            let value = sub.value_of("VALUE").unwrap_or("").to_owned();
            let address = get_address_from_args(sub)?;
            let mut client = KvsClient::connect(address)?;
            let req = Msg::build_bulk_array(&vec!["set".to_owned(), key, value]);
            let res = client.request_msg(req)?;
            match res {
                Msg::Bulk(_) => {
                    // bulk means success
                }
                Msg::Error(e) => {
                    eprintln!("{}", e);
                    exit(1);
                }
                _ => unreachable!()
            }
        }
        ("rm", Some(sub)) => {
            let key = sub.value_of("KEY").unwrap_or("").to_owned();
            let address = get_address_from_args(sub)?;
            let mut client = KvsClient::connect(address)?;
            let req = Msg::build_bulk_array(&vec!["rm".to_owned(), key]);
            let res = client.request_msg(req)?;
            match res {
                Msg::Bulk(_) => {
                    // bulk means success
                }
                Msg::Error(e) => {
                    eprintln!("{}", e);
                    exit(1);
                }
                _ => unreachable!()
            }
        }
        _ => panic!("need least one argument"),
    }
    Ok(())
}

/// IP address format is IP:PORT
fn is_invalid_address(s: &str) -> bool {
    !s.contains(":")
}

/// get ip address from ArgMatches
fn get_address_from_args(arg: &ArgMatches) -> Result<String> {
    let address = arg.value_of("addr").unwrap_or("127.0.0.1:4000");
    if is_invalid_address(address) {
        Err(KvsError::InvalidIPAddressFormat)?
    }
    Ok(address.to_owned())
}