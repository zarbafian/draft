use std::process;

mod config;
mod cluster;
mod socket;

fn main() {

    // Read configuration path
    let config_file = config::get_config_file().unwrap_or_else(|err| {
        eprintln!("Error reading environment variable for configuration: {}", err);
        process::exit(1)
    });

    // Parse configuration file
    let config = config::Config::new(&config_file).unwrap_or_else(|err| {
        println!("Error parsing configuration file: {}", err);
        process::exit(1)
    });

    println!("{:?}", config);

    // Parse members
    let cluster = cluster::parse_members(config.clone());

    println!("cluster: {:?}", cluster);

    // Start listener
    let listener_handle = socket::start_listener(cluster.me.addr);

    // Start cluster thread
    let cluster_handle = cluster::start_cluster_management_thread(cluster);

    listener_handle.join().unwrap();
}
