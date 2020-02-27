use std::process;

mod config;
mod cluster;
mod socket;
mod message;

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

    // Start listener
    let listener_handle = socket::start_listener(cluster.me.addr);

    // Start cluster thread
    let _cluster_handle = cluster::start_cluster_management_thread(cluster);

    listener_handle.join().unwrap();
}

#[cfg(test)]
mod tests{
    use super::*;
    use crate::message::{Request, RequestArgument};

    #[test]
    fn serialize() {
        let mut req = Request::new(String::from("RUN"));
        req.push_arg(RequestArgument{ key: String::from("distance"), value: String::from("5K")});
        req.push_arg(RequestArgument{ key: String::from("allure"), value: String::from("fast")});

        let serialized = message::serialize(req);

        assert!(!serialized.trim().is_empty());
    }

    #[test]
    fn deserialize() {
        let serialized = String::from("{\"action\":\"RUN\",\"arguments\":[{\"key\":\"distance\",\"value\":\"5K\"},{\"key\":\"allure\",\"value\":\"fast\"}]}");
        let deserialized: Request = message::deserialize(&serialized).unwrap();
        assert_eq!(deserialized.action, "RUN");
        assert_eq!(deserialized.arguments.len(), 2);
    }

    #[test]
    fn deserialize_error() {
        // removed a quote between 'action' and 'RUN'
        let serialized = String::from("{\"action:\"RUN\",\"arguments\":[{\"key\":\"distance\",\"value\":\"5K\"},{\"key\":\"allure\",\"value\":\"fast\"}]}");
        assert!(message::deserialize(&serialized).is_err());
    }
}