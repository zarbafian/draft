use serde::{Serialize, Deserialize};
use std::error::Error;

pub fn serialize<T: Serialize>(t: &T) -> Result<String, Box<dyn Error>> {
    match serde_json::to_string(t) {
        Ok(o) => Ok(o),
        Err(e) => Err(e)?,
    }
}

pub fn deserialize<'a, T: Deserialize<'a>>(json: &'a String) -> Result<T, Box<dyn Error>> {
    match serde_json::from_str(json) {
        Ok(o) => Ok(o),
        Err(e) => Err(e)?,
    }
}

#[derive(Debug)]
pub enum Type {
    AppendEntriesRequest,
    AppendEntriesResponse,
    VoteRequest,
    VoteResponse,
    ClientRequest,
    ClientResponse,
}

pub const MESSAGE_TYPE_APPEND_ENTRIES_REQUEST: u8 = 0x01;
pub const MESSAGE_TYPE_VOTE_REQUEST: u8 = 0x02;
pub const MESSAGE_TYPE_CLIENT_REQUEST: u8 = 0x04;

pub const MESSAGE_TYPE_APPEND_ENTRIES_RESPONSE: u8 = 0x11;
pub const MESSAGE_TYPE_VOTE_RESPONSE: u8 = 0x12;
pub const MESSAGE_TYPE_CLIENT_RESPONSE: u8 = 0x14;

pub fn get_type(code: u8) -> Option<Type> {
    match code {
        MESSAGE_TYPE_APPEND_ENTRIES_REQUEST => Some(Type::AppendEntriesRequest),
        MESSAGE_TYPE_APPEND_ENTRIES_RESPONSE => Some(Type::AppendEntriesResponse),
        MESSAGE_TYPE_VOTE_REQUEST => Some(Type::VoteRequest),
        MESSAGE_TYPE_VOTE_RESPONSE => Some(Type::VoteResponse),
        MESSAGE_TYPE_CLIENT_REQUEST => Some(Type::ClientRequest),
        MESSAGE_TYPE_CLIENT_RESPONSE => Some(Type::ClientResponse),
        _ => None
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LogEntry {
    pub term: usize,
    pub index: usize,
    pub data: Query,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientRequest {
    pub client_id: String,
    pub request_id: String,
    pub entry: Query,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct ClientResponse {
    pub server_id: String,
    pub client_id: String,
    pub request_id: String,
    pub result: QueryResult,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct AppendEntriesRequest {
    pub term: usize,
    pub leader_id: String,
    pub prev_log_index: usize,
    pub prev_log_term: usize,
    pub entries: Vec<LogEntry>,
    pub leader_commit: usize,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct AppendEntriesResponse {
    pub sender_id: String,
    pub term: usize,
    pub success: bool,
    pub last_index: usize,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct VoteRequest {
    pub term: usize,
    pub candidate_id: String,
    pub last_log_index: usize,
    pub last_log_term: usize,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct VoteResponse {
    pub voter_id: String,
    pub term: usize,
    pub vote_granted: bool,
}

pub const QUERY_RESULT_SUCCESS: u8 = 0x00;
pub const QUERY_RESULT_REDIRECT: u8 = 0x01;
pub const QUERY_RESULT_CANDIDATE: u8 = 0x12;
pub const QUERY_RESULT_RETRY: u8 = 0x13;

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
pub struct QueryResult {
    error: u8,
    message: String,
    value: String,
}
impl QueryResult {
    pub fn new(error: u8, message: String, value: String) -> QueryResult {
        QueryResult {
            error,
            message,
            value
        }
    }
}
