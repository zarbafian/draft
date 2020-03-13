use std::env;
use std::str::FromStr;
use std::fs;
use std::collections::HashMap;
use std::error::Error;
use std::net::{self, SocketAddr};

const ENV_CONFIG_PATH: &str = "DRAFT_CONFIG";
const PROPERTY_SEPARATOR: &str = "=";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    pub log_file: String,
    pub log_level: String,
    pub handler_threads: usize,
    pub cluster: Cluster,
    pub election_timeout: u64,
    pub election_randomness: u64,
    pub max_inactivity: u64,
}

pub fn get_config() -> Result<Config, Box<dyn Error>> {

    let path = env::var(ENV_CONFIG_PATH)?;

    let properties = read_config(&path)?;

    let members = String::from(properties.get("members").expect("Missing property: members"));
    let me = String::from(properties.get("me").expect("Missing property: me"));

    // Parse cluster
    let cluster = get_cluster(me, members)?;

    Ok(Config{
        log_file: String::from(properties.get("log.filename").expect("Missing property: log.filename")),
        log_level: String::from(properties.get("log.level").expect("Missing property: log.level")),
        handler_threads: properties.get("handler.threads")
            .expect("Missing property: handler.threads")
            .parse()
            .expect("Invalid value for handler threads"),
        cluster,
        election_timeout: properties.get("election.timeout")
            .expect("Missing property: election.timeout")
            .parse()
            .expect("Invalid value for election timeout"),
        election_randomness: properties.get("election.randomness")
            .expect("Missing property: election.randomness")
            .parse()
            .expect("Invalid value for election randomness"),
        max_inactivity: properties.get("max.inactivity")
            .expect("Missing property: max.inactivity")
            .parse()
            .expect("Invalid value for maximum inactivity"),
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

pub fn configure_logging(filename: String, level: String) -> Result<(), Box<dyn Error>>{

    use log::{LevelFilter};
    use log4rs::append::file::FileAppender;
    use log4rs::encode::pattern::PatternEncoder;
    use log4rs::config::{Appender, Config, Root};
    use log4rs::append::console::ConsoleAppender;

    let mut log_levels: HashMap<String, LevelFilter> = HashMap::new();
    log_levels.insert(String::from("OFF"), LevelFilter::Off);
    log_levels.insert(String::from("ERROR"), LevelFilter::Error);
    log_levels.insert(String::from("WARN"), LevelFilter::Warn);
    log_levels.insert(String::from("INFO"), LevelFilter::Info);
    log_levels.insert(String::from("DEBUG"), LevelFilter::Debug);
    log_levels.insert(String::from("TRACE"), LevelFilter::Trace);

    let mut level_filter = &LevelFilter::Info;

    if let Some(filter) = log_levels.get(&level.to_uppercase()) {
        level_filter = filter;
    }
    else {
        Err(format!("Invalid logging level: {}", level))?
    }

    let logfile = FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{d} [{l}] {T} - {m}{n}")))
        .build(filename)?;

    let console = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new("[{l}] {T} - {m}{n}")))
        .build();

    let config = Config::builder()
        .appender(Appender::builder().build("console", Box::new(console)))
        .appender(Appender::builder().build("logfile", Box::new(logfile)))
        .build(Root::builder()
            .appender("console")
            .appender("logfile")
            .build(*level_filter))?;

    log4rs::init_config(config)?;

    Ok(())
}
