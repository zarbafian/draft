use serde::{Serialize, Deserialize};
use std::net::{UdpSocket, SocketAddr};
use std::thread;
use std::sync::Arc;
use log::{debug};

use crate::config::{Config};
use crate::query;

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

#[derive(Serialize, Deserialize, Debug)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub data: String,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct ClientRequest {
    pub client_id: String,
    pub request_id: String,
    pub entry: query::QueryRaw,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct ClientResponse {
    pub server_id: String,
    pub client_id: String,
    pub request_id: String,
    pub result: query::Result,
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
#[derive(Serialize, Deserialize, Debug)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct VoteRequest {
    pub term: u64,
    pub candidate_id: String,
    pub last_log_index: u64,
    pub last_log_term: u64,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct VoteResponse {
    pub voter_id: String,
    pub term: u64,
    pub vote_granted: bool,
}

pub fn broadcast_vote_request(message: VoteRequest, config: &Config) {

    let json: String = serde_json::to_string(&message).unwrap();

    broadcast(MESSAGE_TYPE_VOTE_REQUEST, json, config);
}

pub fn broadcast_append_entries(message: AppendEntriesRequest, config: &Config) {

    let json: String = serde_json::to_string(&message).unwrap();

    broadcast(MESSAGE_TYPE_APPEND_ENTRIES_REQUEST, json, config);
}

pub fn broadcast(message_type: u8, json: String, config: &Config) {

    let data = Arc::new([&[message_type], json.as_bytes()].concat());

    let mut handles = Vec::new();

    for member in &config.cluster.others {

        let address = SocketAddr::from(member.addr);
        let json = json.clone();
        let data = data.clone();

        let handle = thread::Builder::new().name("broadcast".into()).spawn(move ||{
            let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
            let _ = socket.send_to(&data, address).unwrap();

            debug!("Sent vote request to {}: {}", address, json);
        }).unwrap();

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

pub fn send_client_response(message: ClientResponse, recipient: String) {

    let json: String = serde_json::to_string(&message).unwrap();

    send(MESSAGE_TYPE_CLIENT_RESPONSE, json, recipient);
}

pub fn send_vote_response(message: VoteResponse, recipient: String) {

    let json: String = serde_json::to_string(&message).unwrap();

    send(MESSAGE_TYPE_VOTE_RESPONSE, json, recipient);
}

pub fn send(message_type: u8, json: String, recipient: String) {

    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();

    let data = [&[message_type], json.as_bytes()].concat();

    socket.send_to(&data, recipient).unwrap();
}
