use std::env;
use std::str::FromStr;
use std::fs;
use std::collections::HashMap;
use std::error::Error;
use std::net::{self, SocketAddr};

const ENV_CONFIG_PATH: &str = "DRAFT_CONFIG";
const PROPERTY_SEPARATOR: &str = "=";

#[derive(Debug)]
pub struct Member {
    pub addr: net::SocketAddr,
}

#[derive(Debug)]
pub struct Cluster {
    pub me: Member,
    pub others: Vec<Member>,
}

#[derive(Debug)]
pub struct Config {
    pub cluster: Cluster,
    pub election_timout: u64,
    pub election_randomness: u64,
    pub candidate_timout: u64,
    pub candidate_randomness: u64,
}

pub fn get_config() -> Result<Config, Box<dyn Error>> {

    let path = env::var(ENV_CONFIG_PATH)?;

    let properties = read_config(&path)?;

    let members = String::from(properties.get("members").expect("Missing property: members"));
    let me = String::from(properties.get("me").expect("Missing property: me"));

    // Parse cluster
    let cluster = get_cluster(me, members)?;

    Ok(Config{
        cluster,
        election_timout: properties.get("election.timeout")
            .expect("Missing property: election.timeout")
            .parse()
            .expect("Invalid value for election timeout"),
        election_randomness: properties.get("election.randomness")
            .expect("Missing property: election.randomness")
            .parse()
            .expect("Invalid value for election randomness"),
        candidate_timout: properties.get("candidate.timeout")
            .expect("Missing property: candidate.timeout")
            .parse()
            .expect("Invalid value for candidate timeout"),
        candidate_randomness: properties.get("candidate.randomness")
            .expect("Missing property: candidate.randomness")
            .parse()
            .expect("Invalid value for candidate randomness"),
    })
}

fn read_config(path: &String) -> Result<HashMap<String, String>, Box<dyn Error>> {

    let contents = fs::read_to_string(path)?;

    let mut map = HashMap::new();

    for line in contents.lines().filter(|line| { !line.starts_with("#") && !line.trim().is_empty()}) {

        let eq_pos = match line.find(PROPERTY_SEPARATOR) {
            Some(pos) => pos,
            None => Err(format!("Invalid line in configuration: '{}'", line).as_str())?,
        };
        let (key, value) = line.split_at(eq_pos);
        map.insert(String::from(key), String::from(&value[1..]));
    }

    Ok(map)
}

fn get_cluster(my_address: String, all_members: String) -> Result<Cluster, Box<dyn Error>> {

    // Parse list of members
    let members: Vec<&str> = all_members.split(',').collect();

    // Initialize cluster members
    let me = Member {
        addr:
        SocketAddr::from_str(my_address.as_str())?
    };

    let mut others = Vec::new();
    for other in &members {
        if !other.eq(&my_address) {
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