use std::collections::HashMap;
use std::net::{UdpSocket, SocketAddr};
use std::thread;
use std::sync::Arc;

use crate::config::Member;
use crate::message::{self, AppendEntriesRequest, VoteRequest, AppendEntriesResponse, VoteResponse, ClientResponse};

pub fn broadcast_append_entries_request(map: HashMap<Member, AppendEntriesRequest>) {
    let mut handles = Vec::new();

    for (member, request) in map {
        let json: String = message::serialize(&request).unwrap();
        let data = [&[message::MESSAGE_TYPE_APPEND_ENTRIES_REQUEST], json.as_bytes()].concat();

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

    let json: String = message::serialize(&message).unwrap();

    parallel_broadcast(message::MESSAGE_TYPE_VOTE_REQUEST, json, recipients);
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

    let json: String = message::serialize(&message).unwrap();

    send(message::MESSAGE_TYPE_CLIENT_RESPONSE, json, recipient);
}

pub fn send_vote_response(message: VoteResponse, recipient: String) {

    let json: String = message::serialize(&message).unwrap();

    send(message::MESSAGE_TYPE_VOTE_RESPONSE, json, recipient);
}

pub fn send_append_entries_response(message: AppendEntriesResponse, recipient: String) {

    let json: String = message::serialize(&message).unwrap();

    send(message::MESSAGE_TYPE_APPEND_ENTRIES_RESPONSE, json, recipient);
}

fn send(message_type: u8, json: String, recipient: String) {

    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();

    let data = [&[message_type], json.as_bytes()].concat();

    socket.send_to(&data, recipient).unwrap();
}
