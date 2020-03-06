use std::collections::{HashMap, HashSet};
use std::net::{UdpSocket};
use std::sync::{Arc, Mutex, Condvar};
use std::thread;

use crate::config::Config;
use crate::message::{self, AppendEntriesRequest, VoteRequest, LogEntry, VoteResponse};
use std::time::{Duration, Instant};
use rand::Rng;

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

// TODO: handle spurious wakeups
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

    // Start election timeout handling thread for follower
    start_follower_timeout_thread(timeout_pair);

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

                                apply_append_entries_request(&mut server, timeout_cvar, message, thread_pair.clone());

                            },
                            message::Type::VoteRequest => {
                                let message: VoteRequest = parse_json!(&json);
                                println!("Received message: {:?}", message);

                                // Handle vote message
                                let (lock, timeout_cvar) = &*thread_pair;
                                let mut server = lock.lock().unwrap();

                                apply_vote_request(&mut server, timeout_cvar, message);
                            },
                            message::Type::VoteResponse => {
                                let message: VoteResponse = parse_json!(&json);
                                println!("Received message: {:?}", message);

                                let (lock, timeout_cvar) = &*thread_pair;
                                let mut server = lock.lock().unwrap();

                                apply_vote_response(&mut server, &timeout_cvar, message);
                            }
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
    current_votes: HashSet<String>,

    current_term: u64,
    voted_for: Option<String>,
    commit_index: u64,
    last_applied: u64,

    next_index: HashMap<String, u64>,
    match_index: HashMap<String, u64>,

    log: Vec<LogEntry>,
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
            current_votes: HashSet::new(),
            current_term: 0,
            voted_for: None,
            commit_index: 0,
            last_applied: 0,
            next_index,
            match_index,
            log: Vec::new(),
        }
    }

    fn id(&self) -> String {
        return self.config.cluster.me.addr.to_string();
    }

    fn was_elected(&self) -> bool {
        self.current_votes.len() as f64 > ((self.config.cluster.others.len() + 1) as f64 / 2 as f64)
    }
}

/// Apply an 'append entries' message
fn apply_append_entries_request(server: &mut Server, timeout_cvar: &Condvar, request: AppendEntriesRequest, timeout_pair: Arc<(Mutex<Server>, Condvar)>) {

    match server.state {
        ElectionState::Follower => {
            println!("apply_append_entries_request\n{:?}\n{:?}", server, request);
            timeout_cvar.notify_one();
            // TODO: logic
        },
        ElectionState::Candidate => {
            println!("apply_append_entries_request\n{:?}\n{:?}", server, request);
            // TODO: check recency
            if true {
                step_down_as_candidate(server);
                start_follower_timeout_thread(timeout_pair.clone());
            }
        },
        ElectionState::Leader => {

        },
    }
}
/// Handle a vote response
fn apply_vote_response(server: &mut Server, _timeout_cvar: &Condvar, message: VoteResponse) {

    // Check term also
    if message.vote_granted {
        server.current_votes.insert(message.voter_id);
    }
    else {
        // TODO
    }
}

/// Handle a vote request
fn apply_vote_request(server: &mut Server, timeout_cvar: &Condvar, message: VoteRequest) {

    // TODO: check eligibility
    server.voted_for = Some(message.candidate_id.clone());

    // Reset timer
    timeout_cvar.notify_one();

    // Send vote
    message::send_vote(VoteResponse{
        voter_id: server.id(),
        term: server.current_term,
        vote_granted: true,
    }, &message.candidate_id, &server.config);
}

fn apply_election_timeout(server: &mut Server, timeout_pair: Arc<(Mutex<Server>, Condvar)>) {

    if let ElectionState::Leader = server.state {
        // Nothing to do when leader
    }
    else {
        // Candidate or follower: start new election
        println!("apply_election_timeout: {:?} timeout", server.state);

        // Raft algo
        server.current_term += 1;
        server.state = ElectionState::Candidate;
        server.voted_for = Some(server.id());
        let (last_index, last_term) = match server.log.last() {
            Some(e) => (e.index, e.term),
            None => (0, 0),
        };

        // Send vote request to network
        message::broadcast(VoteRequest{
            term: server.current_term,
            candidate_id: server.id(),
            last_log_index: last_index,
            last_log_term: last_term,
        }, &server.config);

        // Start timeout thread
        start_candidate_timeout_thread(timeout_pair);
    }
}
fn step_down_as_candidate(server: &mut Server) {
    println!("step_down_as_candidate");

    // TODO: index, log,...
    server.state = ElectionState::Follower;
    server.voted_for = None;
    server.current_votes = HashSet::new();

}

fn apply_election_won(server: &mut Server) {
    println!("---------------------------------------------");
    println!("---------------------------------------------");
    println!("apply_election_won");
    println!("---------------------------------------------");
    println!("---------------------------------------------");

    server.state = ElectionState::Leader;
    // TODO: send append entries
}

/// Start thread to manage election timeout when candidate
fn start_candidate_timeout_thread(timeout_pair: Arc<(Mutex<Server>, Condvar)>) {

    thread::spawn(move || {
        let mut rand = rand::thread_rng();
        loop {
            println!("-- Election timeout thread - candidate: start");

            let timeout_loop_pair = timeout_pair.clone();
            let (lock, timeout_cvar) = &*timeout_loop_pair;
            let mut server = lock.lock().unwrap();

            println!("-- Election timeout thread - candidate - wait for leader: {}", 0);
            let start = Instant::now();

            let timeout = server.config.election_timout + rand.gen_range(0, server.config.election_randomness);
            let result = timeout_cvar.wait_timeout(server, Duration::from_millis(timeout)).unwrap();

            // Check that we actually reached timeout
            if result.1.timed_out() {
                let duration = start.elapsed();
                println!("-- Election timeout thread - candidate - [ !!! TIMEOUT !!! ] waited: {}", duration.as_millis());

                server = result.0;
                apply_election_timeout(&mut server, timeout_loop_pair.clone());
                break;
            } else {
                println!("-- Election timeout thread - candidate: RESET");
                // TODO: check votes
                server = result.0;
                if server.was_elected() {
                    apply_election_won(&mut server);
                    break;
                } else {
                    // TODO: check logic here
                    continue;
                }
            }
        }
    });
}

/// Start thread to manage election timeout when follower
fn start_follower_timeout_thread(timeout_pair: Arc<(Mutex<Server>, Condvar)>) {

    thread::spawn(move || {
        let mut rand = rand::thread_rng();
        loop {
            println!("-- Election timeout thread - follower: loop start");

            let timeout_loop_pair = timeout_pair.clone();
            let (lock, timeout_cvar) = &*timeout_loop_pair;
            let mut server = lock.lock().unwrap();

            println!("-- Election timeout thread - follower - wait for leader: {}", 0);
            let start = Instant::now();

            let timeout = server.config.election_timout + rand.gen_range(0, server.config.election_randomness);
            let result = timeout_cvar.wait_timeout(server, Duration::from_millis(timeout)).unwrap();

            // Check that we actually reached timeout
            if result.1.timed_out() {

                let duration = start.elapsed();
                println!("-- Election timeout thread - follower - [ !!! TIMEOUT !!! ] waited: {}", duration.as_millis());

                server = result.0;
                apply_election_timeout(&mut server, timeout_loop_pair.clone());
                break;
            }
            else {
                println!("-- Election timeout thread - follower: RESET");
                // TODO: check that an 'append entries' was received
                continue;
            }
        }
    });
}