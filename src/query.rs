use serde::{Serialize, Deserialize};

pub const QUERY_RESULT_SUCCESS: u8 = 0x00;
pub const QUERY_RESULT_REDIRECT: u8 = 0x01;
pub const QUERY_RESULT_CANDIDATE: u8 = 0x10;
pub const QUERY_RESULT_RETRY: u8 = 0x11;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Action {
    Get,
    Post,
    Put,
    Delete,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Query {
    pub action: Action,
    pub key: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Result {
    error: u8,
    message: String,
    value: String,
}
impl Result {
    pub fn new(error: u8, message: String, value: String) -> Result {
        Result{
            error,
            message,
            value
        }
    }
}