use serde::{Serialize, Deserialize};
use std::net::{UdpSocket, SocketAddr};
use std::thread;
use std::sync::Arc;

use crate::config::Member;
use crate::query;
use std::error::Error;
use crate::query::Query;
use std::collections::HashMap;

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
    pub entry: query::Query,
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

pub fn broadcast_append_entries_request(map: HashMap<Member, AppendEntriesRequest>) {
    let mut handles = Vec::new();

    for (member, request) in map {
        let json: String = serialize(&request).unwrap();
        let data = [&[MESSAGE_TYPE_APPEND_ENTRIES_REQUEST], json.as_bytes()].concat();

        let handle = thread::Builder::new()
            .name("broadcast append entries".into())
            .spawn(move ||{
                let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
                let _ = socket.send_to(&data, SocketAddr::from(member.addr)).unwrap();
            })
            .unwrap();

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

pub fn broadcast_vote_request(message: VoteRequest, recipients: Vec<Member>) {

    let json: String = serialize(&message).unwrap();

    parallel_broadcast(MESSAGE_TYPE_VOTE_REQUEST, json, recipients);
}

fn parallel_broadcast(message_type: u8, json: String, recipients: Vec<Member>) {

    let data = Arc::new([&[message_type], json.as_bytes()].concat());

    let mut handles = Vec::new();

    for member in recipients {

        let address = SocketAddr::from(member.addr);
        let data = data.clone();

        let handle = thread::Builder::new()
            .name("broadcast".into())
            .spawn(move ||{
                let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
                let _ = socket.send_to(&data, address).unwrap();
            })
            .unwrap();

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

pub fn send_client_response(message: ClientResponse, recipient: String) {

    let json: String = serialize(&message).unwrap();

    send(MESSAGE_TYPE_CLIENT_RESPONSE, json, recipient);
}

pub fn send_vote_response(message: VoteResponse, recipient: String) {

    let json: String = serialize(&message).unwrap();

    send(MESSAGE_TYPE_VOTE_RESPONSE, json, recipient);
}

pub fn send_append_entries_response(message: AppendEntriesResponse, recipient: String) {

    let json: String = serialize(&message).unwrap();

    send(MESSAGE_TYPE_APPEND_ENTRIES_RESPONSE, json, recipient);
}

fn send(message_type: u8, json: String, recipient: String) {

    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();

    let data = [&[message_type], json.as_bytes()].concat();

    socket.send_to(&data, recipient).unwrap();
}
