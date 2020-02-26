use std::process;

mod config;

fn main() {
    // read configuration path
    let config_file = config::get_config_file().unwrap_or_else(|err| {
        eprintln!("Error reading environment variable: {}", err);
        process::exit(1)
    });
    // parse configuration file
    let config = config::Config::new(&config_file).unwrap_or_else(|err| {
        println!("Error parsing configuration: {}", err);
        process::exit(1)
    });
    println!("{:?}", config);
}
