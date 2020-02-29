mod config;
mod net;

//
mod cluster;
mod socket;
mod raft;
//

use std::process;

fn main() {

    // Parse configuration file
    let config = config::get().unwrap_or_else(|err| {
        eprintln!("Error loading configuration: {}", err);
        process::exit(1)
    });

    // Parse cluster
    let cluster = cluster::get_from(&config).unwrap_or_else(|err| {
        eprintln!("Error with cluster: {}", err);
        process::exit(1)
    });

    // Run
    raft::run(config, cluster);

    // Start listener
    //let listener_handle = socket::start_listener(cluster.me.addr);

    //let raft_handle = raft::start_consensus();

    // Start cluster thread
    //let _cluster_handle = cluster::start_cluster_management_thread(cluster);

    //listener_handle.join().unwrap();
}

#[cfg(test)]
mod tests{
    // TODO
}