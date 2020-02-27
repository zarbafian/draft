use std::env;
use std::fs;
use std::collections::HashMap;
use std::error::Error;

const ENV_CONFIG_PATH: &str = "DRAFT_CONFIG";
const PROPERTY_SEPARATOR: &str = "=";

#[derive(Debug, Clone)]
pub struct Config {
    pub members: String,
    pub me: String,
}

pub fn get() -> Result<Config, Box<dyn Error>> {

    let path = env::var(ENV_CONFIG_PATH)?;

    let properties = read_config(&path)?;

    Ok(Config{
        members: String::from(properties.get("members").expect("Missing property: members")),
        me: String::from(properties.get("me").expect("Missing property: me")),
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