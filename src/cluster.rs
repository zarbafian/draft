use std::net;
use crate::config::Config;
use std::thread::{self,JoinHandle};
use std::net::{SocketAddr};
use std::str::FromStr;
use std::time::Duration;
use rand::Rng;

#[derive(Debug, Clone)]
pub struct Member {
    pub addr: net::SocketAddr,
}

#[derive(Debug)]
pub struct Cluster {
    pub me: Member,
    pub others: Vec<Member>,
}

pub fn parse_members(config: Config) -> Cluster {

    // Parse list of members
    let members: Vec<&str> = config.members.split(',').collect();

    // Initialize cluster members
    let me = Member {
        addr:
        SocketAddr::from_str(config.me.as_str()).expect("Invalid socket address for this member")
    };

    let mut others = Vec::new();
    for other in &members {
        if !other.eq(&config.me) {
            others.push(Member{
                addr:
                SocketAddr::from_str(other).expect("Invalid socket address for an other member")
            });
        }
    }

    Cluster {me, others}
}

pub fn start_cluster_management_thread(cluster: Cluster) -> JoinHandle<()> {

    let handle = thread::spawn(move||{

        // Random generator
        let mut rng = rand::thread_rng();

        // Ping-pong others
        loop {

            // random sleep
            let n: u8 = rng.gen_range(1, 20);

            for _ in 1..(n+1) {
                thread::sleep(Duration::from_secs(1));
                println!("=");
            }

            // TODO: random other member selection
            let other = cluster.others.get(0).unwrap();

            crate::socket::send_message_to(other.addr);
        }
    });

    handle
}
