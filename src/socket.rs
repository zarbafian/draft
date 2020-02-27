use std::thread::{self, JoinHandle};
use std::net::{UdpSocket, SocketAddr};
use crate::message;
use crate::message::Request;

pub fn send_message_to(request: Request, dest: SocketAddr) {

    let json = message::serialize(request);

    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();

    let _ = socket.send_to(&json.as_bytes()[..], dest).unwrap();

    println!("Sent message {} to {}", json, dest)
}

pub fn start_listener(addr: SocketAddr) -> JoinHandle<()> {

    let handle = thread::spawn(move ||{

        // Bind listener socket
        let socket = UdpSocket::bind(addr)
            .expect("could not bind socket");

        let mut buf = [0u8; 65535];

        loop {
            let (amt, src) = socket.recv_from(&mut buf).unwrap();

            let json = String::from_utf8_lossy(&buf[..amt]).to_string();
            println!("received {} bytes from {:?}: {}", amt, src, &json);

            if let Ok(request) = message::deserialize(&json) {
                println!("Recived request: {:?}", request)
            }
            else {
                eprintln!("Listener received invalid request: {}", json);
                continue;
            }
        }
    });

    handle
}