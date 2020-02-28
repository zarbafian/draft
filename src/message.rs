/*
use serde::{Serialize, Deserialize};
use std::error::Error;

#[derive(Serialize, Deserialize, Debug)]
pub struct RequestArgument {
    pub key: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Request {
    pub action: String,
    pub arguments: Vec<RequestArgument>,
}

impl Request {
    pub fn new(action: String) -> Request {
        Request{action, arguments: Vec::new()}
    }
    pub fn push_arg(&mut self, arg: RequestArgument) {
        self.arguments.push(arg);
    }
}

pub fn serialize(request: Request) -> String {
    serde_json::to_string(&request).unwrap()
}

pub fn deserialize(json: &String) -> Result<Request, Box<dyn Error>> {

    Ok(serde_json::from_str(json)?)
}
*/