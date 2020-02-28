mod config;
mod net;

//
mod cluster;
mod socket;
mod raft;
//

use std::process;
use std:: thread;
use std::net::UdpSocket;

use net::message;
use crate::net::message::AppendEntriesRequest;

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

    // Listen of incoming requests
    let main_handle = thread::Builder::new().name("main loop".into()).spawn(move ||{

        // Bind listener socket
        let socket = UdpSocket::bind(cluster.me.addr)
            .expect("could not bind listener socket");

        // Receive buffer
        let mut buf = [0u8; 65535];

        loop {
            if let Ok((amt, src)) = socket.recv_from(&mut buf) {

                // Messages should have at least a message type and one byte of data
                if amt > 1 {

                    // Copy data from buffer so it can be used for another request
                    let mut data = [0u8; 65535];
                    data.copy_from_slice(&buf[..]);

                    thread::spawn(move || {

                        println!("Received {} bytes from {:?}", amt, src);

                        if let Some(message_type) = message::get_type(data[0]) {

                            let json: String = String::from_utf8_lossy(&data[1..amt]).to_string();

                            match message_type {
                                message::Type::AppendEntriesRequest => {
                                    let request: AppendEntriesRequest = match serde_json::from_str(&json) {
                                        Ok(r) => r,
                                        Err(err) => {
                                            eprintln!("Request could not be parsed: {}", err);
                                            return;
                                        },
                                    };
                                },
                                _ => ()
                            }

                            //(message_handler)(message_type, String::from_utf8_lossy(&data[1..amt]).to_string());
                        } else {
                            eprintln!("Received invalid message type: {}", data[0]);
                        }
                    });
                }
                else {
                    eprintln!("Received empty message");
                }
            }
            else {
                eprintln!("Error while receiving from socket");
            }
        }
    }).unwrap();

    main_handle.join().unwrap();

    // Start listener
    //let listener_handle = socket::start_listener(cluster.me.addr);

    //let raft_handle = raft::start_consensus();

    // Start cluster thread
    //let _cluster_handle = cluster::start_cluster_management_thread(cluster);

    //listener_handle.join().unwrap();
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