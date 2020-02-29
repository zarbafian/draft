use std::thread::{self, JoinHandle};
use std::net::UdpSocket;

use crate::net::message::{self, AppendEntriesRequest};
use crate::config::Config;
use crate::cluster::Cluster;
use std::time::{Duration, Instant};
use std::sync::{Arc, Mutex, Condvar};

pub fn run(config: Config, cluster: Cluster) {

    let state_machine = StateMachine::new(config);

    // Bind listener socket
    let socket = UdpSocket::bind(cluster.me.addr)
        .expect("could not bind listener socket");

    // Receive buffer
    let mut buf = [0u8; 65535];

    // Condition variable for hearbeat received
    let original_pair = Arc::new((Mutex::new(state_machine), Condvar::new()));
    let behavior_pair = original_pair.clone();

    {
        // Start consensus
        let (lock, cvar) = &*original_pair;
        let state_machine = lock.lock().unwrap();
        let behavior = state_machine.start_behavior(behavior_pair);
    }

    loop {
        // Clone at each loop so it can move in the thread closure
        let moved_loop_pair = original_pair.clone();

        if let Ok((amt, src)) = socket.recv_from(&mut buf) {

            // Messages should have at least two bytes
            if amt > 1 {

                // Copy data from buffer so it can be used for another request
                let mut data = [0u8; 65535];
                data.copy_from_slice(&buf[..]);

                thread::spawn(move || {

                    println!("Received {} bytes from {:?}", amt, src);

                    if let Some(message_type) = message::get_type(data[0]) {

                        let json: String = String::from_utf8_lossy(&data[1..amt]).to_string();

                        match message_type {
                            message::Type::AppendEntriesRequest => {
                                let request: AppendEntriesRequest = match serde_json::from_str(&json) {
                                    Ok(r) => r,
                                    Err(err) => {
                                        eprintln!("Request could not be parsed: {}", err);
                                        return;
                                    },
                                };
                                println!("Received message: {:?}", request);

                                // Notify of received heartbeat
                                let (lock, cvar) = &*moved_loop_pair;
                                let mut state_machine = lock.lock().unwrap();
                                state_machine.heartbeat_received = true;
                                cvar.notify_one();
                            },
                            _ => ()
                        }

                    }
                });
            }
        }
    }
}

#[derive(Debug)]
enum ElectionState {
    Follower,
    Candidate,
    Leader
}

struct StateMachine {
    election_timout: u64,
    current_state: ElectionState,
    heartbeat_received: bool,
}

impl StateMachine {

    fn new(config: Config) -> StateMachine {
        StateMachine {
            election_timout: config.election_timout,
            current_state: ElectionState::Follower,
            heartbeat_received: false,
        }
    }

    fn start_behavior(&self, election_timeout_pair: Arc<(Mutex<StateMachine>, Condvar)>) -> JoinHandle<()> {

        let timeout = self.election_timout;

        thread::spawn(move || {

            let (lock, cvar) = &*election_timeout_pair;

            loop {
                println!("-- election: loop start");

                let mut state_machine = lock.lock().unwrap();

                println!("-- election - wait for heartbeat: {}", 0);
                let start = Instant::now();

                // TODO: use self.election_timout
                //let result = cvar.wait_timeout(state_machine, Duration::from_millis(5000)).unwrap();
                let result = cvar.wait_timeout(state_machine, Duration::from_millis(timeout)).unwrap();

                let duration = start.elapsed();
                println!("-- election - waited: {}", duration.as_millis());

                state_machine = result.0;

                if state_machine.heartbeat_received == true {
                    println!("-- election: received HEARTBEAT -|v-|v-|v-|v-|v-|v-|v");
                    // Reset heartbeat timeout
                    state_machine.heartbeat_received = false;
                    continue;
                }
                else {
                    println!("-- election - timeout! Viva la revolucion!");
                    break;
                }
            }

            println!("----- end follower, begin candidate");
        })
    }
}