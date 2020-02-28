use serde::{Serialize, Deserialize};

#[derive(Debug)]
pub enum Type {
    AppendEntriesRequest,
    AppendEntriesResponse,
    EntriesRequest,
    EntriesResponse,
    VoteRequest,
    VoteResponse,
    ClientRequest,
    ClientResponse,
}

pub const MESSAGE_TYPE_APPEND_ENTRIES_REQUEST: u8 = 0x01;
pub const MESSAGE_TYPE_ENTRIES_REQUEST: u8 = 0x02;
pub const MESSAGE_TYPE_VOTE_REQUEST: u8 = 0x04;
pub const MESSAGE_TYPE_CLIENT_REQUEST: u8 = 0x08;

pub const MESSAGE_TYPE_APPEND_ENTRIES_RESPONSE: u8 = 0x11;
pub const MESSAGE_TYPE_ENTRIES_RESPONSE: u8 = 0x12;
pub const MESSAGE_TYPE_VOTE_RESPONSE: u8 = 0x14;
pub const MESSAGE_TYPE_CLIENT_RESPONSE: u8 = 0x18;

pub fn get_type(code: u8) -> Option<Type> {
    match code {
        MESSAGE_TYPE_APPEND_ENTRIES_REQUEST => Some(Type::AppendEntriesRequest),
        MESSAGE_TYPE_APPEND_ENTRIES_RESPONSE => Some(Type::AppendEntriesResponse),
        MESSAGE_TYPE_ENTRIES_REQUEST => Some(Type::EntriesRequest),
        MESSAGE_TYPE_ENTRIES_RESPONSE => Some(Type::EntriesResponse),
        MESSAGE_TYPE_VOTE_REQUEST => Some(Type::VoteRequest),
        MESSAGE_TYPE_VOTE_RESPONSE => Some(Type::VoteResponse),
        MESSAGE_TYPE_CLIENT_REQUEST => Some(Type::ClientRequest),
        MESSAGE_TYPE_CLIENT_RESPONSE => Some(Type::ClientResponse),
        _ => None
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct LogEntry {
    pub data: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AppendEntriesRequest {
    pub term: u64,
    pub leader_id: String,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}
