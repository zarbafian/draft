use std::collections::HashMap;
use std::net::{UdpSocket};
use std::sync::{Arc, Mutex, Condvar};
use std::thread;

use crate::config::Config;
use crate::message::{self, AppendEntriesRequest};
use std::time::{Duration, Instant};

macro_rules! parse_json {
    ($x:expr) => {
        match serde_json::from_str($x) {
            Ok(r) => r,
            Err(err) => {
                eprintln!("Could not parse JSON message: {}", err);
                return;
            },
        }
    };
}

pub fn start(config: Config) {

    // Copy my address before it is moved in Server
    let my_address = config.cluster.me.addr.clone();

    println!("");
    println!("[{}] server started", my_address);
    println!("");

    let server = Server::new(config);
    println!("{:?}", server);

    // Bind listener socket
    let socket = UdpSocket::bind(my_address)
        .expect("could not bind listener socket");

    // Receive buffer
    let mut buf = [0u8; 65535];

    // Shared data: server state machine + condition variable election timeout
    let shared_pair = Arc::new((Mutex::new(server), Condvar::new()));
    let timeout_pair = shared_pair.clone();

    // Start election timeout handling thread
    start_timeout_thread(timeout_pair);

    loop {
        if let Ok((amount, src)) = socket.recv_from(&mut buf) {

            // Messages should have at least two bytes
            if amount > 1 {

                // Copy data from buffer so the buffer can be used for another request
                let mut data = [0u8; 65535];
                data.copy_from_slice(&buf[..]);

                let thread_pair = shared_pair.clone();

                // Message handling thread
                thread::spawn(move || {

                    println!("Received {} bytes from {:?}", amount, src);

                    // Check that we received a valid message type in the first byte
                    if let Some(message_type) = message::get_type(data[0]) {
                        let json: String = String::from_utf8_lossy(&data[1..amount]).to_string();

                        match message_type {
                            message::Type::AppendEntriesRequest => {
                                let message: AppendEntriesRequest = parse_json!(&json);
                                println!("Received message: {:?}", message);

                                let (lock, timeout_cvar) = &*thread_pair;
                                let mut server = lock.lock().unwrap();

                                apply_append_entries_request(&mut server, timeout_cvar, message);

                            },
                            /*
                            message::Type::VoteRequest => {
                                let message: VoteRequest = parse_json!(&json);
                                println!("Received message: {:?}", message);

                                // Handle vote message
                                let (lock, _follower_cvar, _candidate_cvar) = &*moved_loop_pair;
                                let mut state_machine = lock.lock().unwrap();

                                state_machine.apply_vote_request_message(message);
                            },
                            message::Type::VoteResponse => {
                                let message: VoteResponse = parse_json!(&json);
                                println!("Received message: {:?}", message);

                                let (lock, _follower_cvar, candidate_cvar) = &*moved_loop_pair;
                                let mut state_machine = lock.lock().unwrap();

                                //state_machine.heartbeat_received = true;
                                state_machine.apply_vote_response_message(message);

                                if state_machine.has_won_election() {
                                    // Notify of won election
                                    candidate_cvar.notify_one();
                                }
                            }
                            */
                            _ => unimplemented!()
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

#[derive(Debug)]
struct Server {
    config: Config,

    state: ElectionState,

    current_term: u64,
    voted_for: Option<String>,
    commit_index: u64,
    last_applied: u64,

    next_index: HashMap<String, u64>,
    match_index: HashMap<String, u64>,
}

impl Server {
    /// Create a new server
    fn new(config: Config) -> Server {

        // Initialize indices to 0
        let zeros_next = vec![0; config.cluster.others.len()];
        let zeros_match = vec![0; config.cluster.others.len()];
        let next_index = config.cluster.others.iter().map(|o| o.addr.to_string()).zip(zeros_next).collect();
        let match_index = config.cluster.others.iter().map(|o| o.addr.to_string()).zip(zeros_match).collect();

        Server {
            config,
            state: ElectionState::Follower,
            current_term: 0,
            voted_for: None,
            commit_index: 0,
            last_applied: 0,
            next_index,
            match_index,
        }
    }
}

fn apply_append_entries_request(server: &mut Server, timeout_cvar: &Condvar, request: AppendEntriesRequest) {

    match server.state {
        ElectionState::Follower => {
            println!("apply_append_entries_request\n{:?}\n{:?}", server, request);
            timeout_cvar.notify_one();
        },
        ElectionState::Candidate => {

        },
        ElectionState::Leader => {

        },
    }
}
fn apply_election_timeout(server: &mut Server) {
    match server.state {
        ElectionState::Follower => {
            println!("follower timeout");
        },
        ElectionState::Candidate => {
            println!("candidate timeout");
        },
        _ => (),
    }
}

fn start_timeout_thread(timeout_pair: Arc<(Mutex<Server>, Condvar)>) {

    thread::spawn(move || {

        loop {
            println!("-- Election timeout thread: loop start");

            let timeout_loop_pair = timeout_pair.clone();
            let (lock, timeout_cvar) = &*timeout_loop_pair;
            let mut server = lock.lock().unwrap();

            println!("-- Election timeout thread - wait for heartbeat: {}", 0);
            let start = Instant::now();

            let timeout = server.config.election_timout;
            let result = timeout_cvar.wait_timeout(server, Duration::from_millis(timeout)).unwrap();

            // Check that we actually reached timeout
            if result.1.timed_out() {

                let duration = start.elapsed();
                println!("-- Election timeout thread - [ !!! TIMEOUT !!! ] waited: {}", duration.as_millis());

                server = result.0;
                apply_election_timeout(&mut server);
                break;
            }
            else {
                println!("-- Election timeout thread: RESET");
                continue;
            }
        }
    });
}