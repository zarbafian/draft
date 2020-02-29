use std::net::{self, SocketAddr};
use std::str::FromStr;
use std::error::Error;

use crate::config::Config;

/*
use std::thread::{self,JoinHandle};
use std::time::Duration;
use rand::Rng;
*/

#[derive(Debug, Clone)]
pub struct Member {
    pub addr: net::SocketAddr,
}

#[derive(Debug)]
pub struct Cluster {
    pub me: Member,
    pub others: Vec<Member>,
}

pub fn get_from(config: &Config) -> Result<Cluster, Box<dyn Error>> {

    // Parse list of members
    let members: Vec<&str> = config.members.split(',').collect();

    // Initialize cluster members
    let me = Member {
        addr:
        SocketAddr::from_str(config.me.as_str())?
    };

    let mut others = Vec::new();
    for other in &members {
        if !other.eq(&config.me) {
            others.push(Member{
                addr:
                SocketAddr::from_str(other)?
            });
        }
    }

    if members.len() != 1 + others.len() {
        Err("Members count should be equal to one plus the count of others")?
    }

    Ok(Cluster {me, others})
}
/*
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

            // Randomly select another member
            let other_index = rng.gen_range(0, cluster.others.len());
            let other = cluster.others.get(other_index).unwrap();

            /*
            TODO
            // Create message
            let message = rng.gen_range(0, 1000000);
            let mut request = AppendEntriesRequest::new(String::from("PING"));
            request.push_arg(RequestArgument{key: String::from("ID"), value: message.to_string()});

            crate::socket::send_message_to(request, other.addr);
            */
        }
    });

    handle
}
*/