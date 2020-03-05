mod config;
mod server;
mod message;
mod raft;

use std::process;

fn main() {

    // Parse configuration file
    let config = config::get_config().unwrap_or_else(|err| {
        eprintln!("Error loading configuration: {}", err);
        process::exit(1)
    });

    // Run
    //raft::run(config);
    server::start(config);
}

#[cfg(test)]
mod tests{
    // TODO
}