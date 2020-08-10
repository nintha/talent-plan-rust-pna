#[macro_use]
extern crate clap;

use clap::App;

fn main() {
    // The YAML file is found relative to the current file, similar to how modules are found
    let yaml = load_yaml!("cli.yml");
    let m = App::from(yaml)
        .name(crate_name!())
        .version(crate_version!())
        .get_matches();

    match m.subcommand() {
        ("get", Some(_)) => panic!("unimplemented"),
        ("set", Some(_)) => panic!("unimplemented"),
        ("rm", Some(_)) => panic!("unimplemented"),
        _ => unreachable!(),
    }
}
