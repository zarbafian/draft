use std::collections::{HashMap, HashSet};
use std::net::{UdpSocket};
use std::sync::{Arc, Mutex, Condvar};
use std::thread;
use rand::Rng;
use std::time::{Duration, Instant};
use std::borrow::Borrow;
use log::{trace, debug, info, error};

use crate::query;
use crate::config::{Config, Member};
use crate::message::{self, AppendEntriesRequest, VoteRequest, LogEntry, VoteResponse, broadcast_append_entries, AppendEntriesResponse, ClientRequest, ClientResponse};
use crate::query::Query;

macro_rules! parse_json {
    ($x:expr) => {
        match message::deserialize($x) {
            Ok(r) => r,
            Err(err) => {
                error!("Could not parse JSON message: {}", err);
                return;
            },
        }
    };
}

pub fn start(config: Config) {

    // Copy my address before it is moved in Server
    let my_address = config.cluster.me.addr.clone();

    info!("");
    info!("[{}] server started", my_address);
    info!("");

    let server = Server::new(config);
    debug!("{:?}", server);

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
                thread::Builder::new()
                    .name("message handler".into())
                    .spawn(move || {
                        trace!("Received {} bytes from {:?}", amount, src);

                        // Check that we received a valid message type in the first byte
                        if let Some(message_type) = message::get_type(data[0]) {
                            let json: String = String::from_utf8_lossy(&data[1..amount]).to_string();

                            match message_type {
                                message::Type::AppendEntriesRequest => {
                                    let message: AppendEntriesRequest = parse_json!(&json);
                                    debug!("Received message: {:?}", message);

                                    let (lock, timeout_cvar) = &*thread_pair;
                                    let mut server = lock.lock().unwrap();

                                    if server.apply_append_entries_request(message) {
                                        // Received a heartbeat: reset election timeout, step down from candidate or leader
                                        timeout_cvar.notify_one();
                                    }
                                },
                                message::Type::AppendEntriesResponse => {
                                    let message: AppendEntriesResponse = parse_json!(&json);
                                    debug!("Received message: {:?}", message);

                                    let (lock, _timeout_cvar) = &*thread_pair;
                                    let mut server = lock.lock().unwrap();

                                    server.apply_append_entries_response(message);
                                },
                                message::Type::VoteRequest => {
                                    let message: VoteRequest = parse_json!(&json);
                                    debug!("Received message: {:?}", message);

                                    // Handle vote message
                                    let (lock, timeout_cvar) = &*thread_pair;
                                    let mut server = lock.lock().unwrap();

                                    if server.apply_vote_request(message) {
                                        // Sent a vote
                                        timeout_cvar.notify_one();
                                    }
                                },
                                message::Type::VoteResponse => {
                                    let message: VoteResponse = parse_json!(&json);
                                    debug!("Received message: {:?}", message);

                                    let (lock, timeout_cvar) = &*thread_pair;
                                    let mut server = lock.lock().unwrap();

                                    server.apply_vote_response(message);
                                    timeout_cvar.notify_one();
                                }
                                message::Type::ClientRequest => {
                                    let message: ClientRequest = parse_json!(&json);
                                    debug!("Received message: {:?}", message);

                                    let (lock, _timeout_cvar) = &*thread_pair;
                                    let mut server = lock.lock().unwrap();

                                    server.apply_client_request(message);
                                    //timeout_cvar.notify_one();
                                },
                                _ => error!("Unhandled message type: {:?}", message_type)
                            }

                        }
                    }).unwrap();
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

    leader_id: Option<String>,
    reset_election_timeout: bool,

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
            leader_id: None,
            reset_election_timeout: true,
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

    fn apply_append_entries_request(&mut self, request: AppendEntriesRequest) -> bool {

        match self.state {
            ElectionState::Follower => {
                if request.term < self.current_term {
                    // Bad term
                    return false;
                }

                // TODO: check validity
                self.reset_election_timeout = true;
                self.voted_for = None;
                self.leader_id = Some(request.leader_id);
                true
            },
            ElectionState::Candidate => {
                // TODO: check validity
                // If request.term > current_term then convert to follower
                self.voted_for = None;
                true
            },
            ElectionState::Leader => {
                // TODO: check validity
                // If request.term > current_term then convert to follower
                self.voted_for = None;
                true
            }
        }
    }

    fn apply_append_entries_response(&mut self, _response: AppendEntriesResponse) {
        unimplemented!();
    }

    /// Handle a vote request
    fn apply_vote_request(&mut self, request: VoteRequest) -> bool{

        if let Some(votee) = self.voted_for.borrow() {
            if !votee.eq(&request.candidate_id) {
                // Already voted for someone else
                info!("already voted for: {}", votee);
                return false;
            }
        }

        // Find votee
        let mut votee: Option<Member> = None;

        for other in self.config.cluster.others.iter() {
            if other.addr.to_string().eq(&request.candidate_id) {
                votee = Some(Member{addr: other.addr.clone()});
                break;
            }
        }

        if let Some(m) = votee {

            // TODO: check eligibility
            let vote_request_accepted = true;

            if vote_request_accepted {
                info!("Vote accepted: {:?}", request);
                self.voted_for = Some(request.candidate_id.clone());
                self.reset_election_timeout = true;

                let voter_id = self.id();
                let term = self.current_term;
                let vote_granted = true;
                thread::Builder::new().name("send granted vote".into()).spawn(move || {
                    // Send vote
                    message::send_vote_response(VoteResponse {
                        voter_id,
                        term,
                        vote_granted,
                    }, m.addr.to_string());
                }).unwrap();
                // Vote sent
                true

            }
            else {
                // Vote request rejected
                info!("Vote rejected: {:?}", request);
                let voter_id = self.id();
                let term = self.current_term;
                let vote_granted = false;
                thread::Builder::new().name("send rejected vote".into()).spawn(move || {
                    // Send vote
                    message::send_vote_response(VoteResponse {
                        voter_id,
                        term,
                        vote_granted,
                    }, m.addr.to_string());
                }).unwrap();
                false
            }
        }
        else {
            // Candidate not found
            error!("Member not found for sending vote: {:?}", request.candidate_id);
            false
        }
    }

    /// Handle a vote response
    fn apply_vote_response(&mut self, response: VoteResponse) {

        // Check term also
        if response.vote_granted {
            self.current_votes.insert(response.voter_id);
        }
        else {
            // TODO
        }
    }

    /// Handle client request
    fn apply_client_request(&mut self, message: ClientRequest) {

        info!("--- apply_client_request");

        match self.state {
            ElectionState::Leader => {
                // Handle message
                info!("--- apply_client_request: LEADER");
                if let Some(q) = message.entry.to_query() {
                    // Valid message
                    self.append_log_entries(vec![q]);

                    let result = query::Result::new(query::QUERY_RESULT_SUCCESS, "success".to_string(), "".to_string());
                    send_client_response(self.id(), message, result);
                }
                else {
                    // Invalid message
                    info!("invalid query: {:?}", message.entry);
                    let result = query::Result::new(query::QUERY_RESULT_INVALID_QUERY, "invalid query".to_string(), "".to_string());
                    send_client_response(self.id(), message, result);
                }
            },
            ElectionState::Follower => {
                if let Some(leader) = self.leader_id.borrow() {
                    // Redirect to leader by sending its address
                    let result = query::Result::new(query::QUERY_RESULT_REDIRECT, "leader redirect".to_string(), leader.to_string());
                    send_client_response(self.id(), message, result);
                }
                else {
                    // Currently no leader to handle the request
                    let result = query::Result::new(query::QUERY_RESULT_RETRY, "leader unknown".to_string(), "".to_string());
                    send_client_response(self.id(), message, result);
                }
            }
            ElectionState::Candidate => {
                // Leader offline, proceeding with election
                let result = query::Result::new(query::QUERY_RESULT_CANDIDATE, "leader offline".to_string(), "".to_string());
                send_client_response(self.id(), message, result);
            }
        }
    }

    fn append_log_entries(&mut self, queries: Vec<Query>) {

        for q in queries {
            // TODO: handle state machine
            info!("will append log: {:?}", q);
            self.log.push(LogEntry {
                term: self.current_term,
                index: self.log.len() as u64 + 1,
                data: q,
            });
        }
    }
}

fn send_client_response(server_id: String, request: ClientRequest, result: query::Result) {
    thread::Builder::new()
        .name("client response".into())
        .spawn(move || {
            let response = ClientResponse {
                server_id,
                client_id: request.client_id.clone(),
                request_id: request.request_id.clone(),
                result
            };
            debug!("Sent client response to {:?}: {:?}", request.client_id, response);
            message::send_client_response(response, request.client_id);
        })
        .unwrap();
}

fn handle_election_timeout(timeout_pair: Arc<(Mutex<Server>, Condvar)>) {

    let (lock, _timeout_cvar) = &*timeout_pair;
    let mut server = lock.lock().unwrap();

    if let ElectionState::Leader = server.state {
        // Nothing to do when leader
    }
    else {
        // Candidate or follower: start new election
        info!("handle_election_timeout: {:?} timeout", server.state);

        let my_id = server.id();
        // Raft algo
        server.current_term += 1;
        server.state = ElectionState::Candidate;
        server.voted_for = Some(my_id.clone());
        server.current_votes = HashSet::new();
        server.current_votes.insert(my_id);
        let (last_index, last_term) = match server.log.last() {
            Some(e) => (e.index, e.term),
            None => (0, 0),
        };

        // Send vote request to network
        message::broadcast_vote_request(VoteRequest{
            term: server.current_term,
            candidate_id: server.id(),
            last_log_index: last_index,
            last_log_term: last_term,
        }, &server.config);

        // Start timeout thread
        start_candidate_timeout_thread(timeout_pair.clone());
    }
}
fn handle_step_down(timeout_pair: Arc<(Mutex<Server>, Condvar)>) {
    info!("handle_step_down");

    let (lock, _timeout_cvar) = &*timeout_pair;
    let mut server = lock.lock().unwrap();

    // TODO: index, log,...
    server.state = ElectionState::Follower;
    server.voted_for = None;
    server.current_votes = HashSet::new();

    start_follower_timeout_thread(timeout_pair.clone());
}

fn handle_election_won(timeout_pair: Arc<(Mutex<Server>, Condvar)>) {
    info!("---------------------------------------------");
    info!("------------ handle_election_won ------------");
    info!("---------------------------------------------");

    let (lock, _timeout_cvar) = &*timeout_pair;
    let mut server = lock.lock().unwrap();

    server.state = ElectionState::Leader;

    start_leader_thread(timeout_pair.clone());
}

fn start_leader_thread(timeout_pair: Arc<(Mutex<Server>, Condvar)>) {
    thread::Builder::new()
        .name(String::from("leader"))
        .spawn(move || {
            info!("started");
            loop {
                let (lock, timeout_cvar) = &*timeout_pair.clone();
                let server = lock.lock().unwrap();

                broadcast_append_entries(AppendEntriesRequest{
                    term: server.current_term,
                    leader_id: server.id(),
                    prev_log_index: 0, // TODO
                    prev_log_term: 0, // TODO
                    entries: vec![], // TODO
                    leader_commit: 0, // TODO
                }, &server.config);

                let sleep = (server.config.election_timeout as f64 / 2 as f64) as u64;
                debug!("Leader will sleep for {} ms", sleep);

                // TODO: use another condvar?
                let _ = timeout_cvar.wait_timeout(server, Duration::from_millis(sleep));
                //thread::sleep(Duration::from_millis(sleep));
            }
        })
        .unwrap();
}

/// Start thread to manage election timeout when candidate
fn start_candidate_timeout_thread(timeout_pair: Arc<(Mutex<Server>, Condvar)>) {

    thread::Builder::new()
        .name(String::from("candidate"))
        .spawn(move || {
            info!("started");
            let mut rand = rand::thread_rng();
            let timeout;
            {
                let (lock, _timeout_cvar) = &*timeout_pair;
                let server = lock.lock().unwrap();
                timeout = server.config.election_timeout + rand.gen_range(0, server.config.election_randomness);
            }

            let start = Instant::now();
            let mut elapsed = 0;

            // How the candidate thread may end
            let election_timeout_occured;
            let has_won_election;
            let other_leader_elected;


            trace!("election timeout - will wait with timeout={}ms", timeout);

            loop {
                let (lock, timeout_cvar) = &*timeout_pair;
                let mut server = lock.lock().unwrap();

                let result = timeout_cvar.wait_timeout(server, Duration::from_millis(timeout - elapsed)).unwrap();

                elapsed = start.elapsed().as_millis() as u64;
                server = result.0;

                if server.was_elected() {
                    // Was elected leader
                    info!("election timeout: election won");
                    has_won_election = true;
                    election_timeout_occured = false;
                    other_leader_elected = false;
                    break;
                }
                else if server.reset_election_timeout {
                    // Heartbeat received: another leader was elected
                    info!("election timeout: received message from leader, will step down");
                    server.reset_election_timeout = false;

                    has_won_election = false;
                    election_timeout_occured = false;
                    other_leader_elected = true;
                    break;
                }
                else if result.1.timed_out() || elapsed >= timeout {
                    // Timeout reached
                    info!("election timeout of {}: timout_out()={}, elapsed={}", timeout, result.1.timed_out(), elapsed);
                    has_won_election = false;
                    election_timeout_occured = true;
                    other_leader_elected = false;
                    break;
                }
                else {
                    // Neither timeout nor winning or losing election: keep waiting
                    continue;
                }
            }

            if election_timeout_occured {
                handle_election_timeout(timeout_pair.clone());
            }
            else if has_won_election {
                handle_election_won(timeout_pair.clone());
            }
            else if other_leader_elected {
                handle_step_down(timeout_pair.clone());
            }
        })
        .unwrap();
}

/// Start thread to manage election timeout when follower
fn start_follower_timeout_thread(timeout_pair: Arc<(Mutex<Server>, Condvar)>) {

    thread::Builder::new()
        .name(String::from("follower"))
        .spawn(move || {
            debug!("started");
            let mut rand = rand::thread_rng();

            // Indicate an election timeout
            let mut election_timeout_occured;

            // Outer loop for handling heartbeats
            loop {
                trace!("election timeout: loop start");

                let timeout_loop_pair = timeout_pair.clone();

                let (lock, timeout_cvar) = &*timeout_loop_pair;
                let mut server = lock.lock().unwrap();

                // Randomize election timeout
                let timeout = server.config.election_timeout + rand.gen_range(0, server.config.election_randomness);

                trace!("election timeout: will wait {} ms for heartbeat", timeout);

                let start = Instant::now();
                let mut elapsed = 0;

                // Inner loop to make sure we waited the full length of the timeout
                loop {
                    let result = timeout_cvar.wait_timeout(server, Duration::from_millis(timeout - elapsed)).unwrap();

                    elapsed = start.elapsed().as_millis() as u64;
                    server = result.0;

                    if server.reset_election_timeout {
                        // Heartbeat received: reset flag
                        server.reset_election_timeout = false;
                        election_timeout_occured = false;
                        break; // inner loop
                    }
                    if result.1.timed_out() || elapsed >= timeout {
                        // Timeout reached
                        info!("election timeout: timout_out()={}, elapsed={}", result.1.timed_out(), elapsed);
                        election_timeout_occured = true;
                        break; // inner loop
                    }
                    else {
                        // Neither timeout nor heartbeat: keep waiting
                        continue;
                    }
                }

                if election_timeout_occured {
                    // Timeout occured
                    break; // outer loop
                }
                else {
                    // Heartbeat received
                    continue;
                }
            }

            if election_timeout_occured {
                handle_election_timeout(timeout_pair.clone());
            }
        })
        .unwrap();
}