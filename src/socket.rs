use std::thread::{self, JoinHandle};
use std::net::{UdpSocket, SocketAddr};
use rand::Rng;

pub fn send_message_to(addr: SocketAddr) {

    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();

    let msg = format!("MSG_{}", rand::thread_rng().gen_range(0, 100000));

    let res = socket.send_to(&msg.as_bytes()[..], addr).unwrap();

    println!("Sent message {} to {}", msg, addr)
}

pub fn start_listener(addr: SocketAddr) -> JoinHandle<()> {

    let handle = thread::spawn(move ||{

        // Bind listener socket
        let socket = UdpSocket::bind(addr)
            .expect("could not bind socket");

        let mut buf = [0u8; 65535];

        loop {
            let (amt, src) = socket.recv_from(&mut buf).unwrap();
            println!("received {} bytes from {:?}: {}", amt, src, String::from_utf8_lossy(&buf[..amt]));
        }
    });

    handle
}