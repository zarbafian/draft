use serde::{Serialize, Deserialize};
use std::error::Error;
use std::net::{UdpSocket, SocketAddr};
use std::thread;

use crate::config::{Config, Member};
use std::sync::Arc;

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

trait Message<T> {
    fn to_json(&self) -> Result<String, Box<dyn Error>>;
    fn from_json(json: &String) -> Result<T, Box<dyn Error>>;
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
    pub term: u64,
    pub term_granted: bool,
}

pub fn broadcast(message: VoteRequest, config: &Config) {

    let json: String = serde_json::to_string(&message).unwrap();

    let data = Arc::new([&[MESSAGE_TYPE_VOTE_REQUEST], json.as_bytes()].concat());

    let mut handles = Vec::new();

    for member in &config.cluster.others {

        let address = SocketAddr::from(member.addr);
        let json = json.clone();
        let data = data.clone();

        let handle = thread::spawn(move ||{
            let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
            let _ = socket.send_to(&data, address).unwrap();

            println!("Sent vote request to {}: {}", address, json);
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

pub fn send_vote(message: VoteResponse, recipient: &String, config: &Config) {

    let json: String = serde_json::to_string(&message).unwrap();

    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();

    let data = [&[MESSAGE_TYPE_VOTE_RESPONSE], json.as_bytes()].concat();

    let mut member: Option<&Member> = None;

    for other in &config.cluster.others {
        if other.addr.to_string().eq(recipient) {
            member = Some(other);
            break;
        }
    }

    if let Some(m) = member {
        socket.send_to(&data, m.addr).unwrap();
        println!("Sent vote request to {}: {}", m.addr, json);
    }
    else {
        eprintln!("Member not found for sending vote: {:?}", recipient);
    }
}
