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
use crate::message::{self, AppendEntriesRequest, VoteRequest, LogEntry, VoteResponse, AppendEntriesResponse, ClientRequest, ClientResponse};
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
    // Document use with the naming:
    // - NAME-ACTION
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
                                        server.reset_election_timeout = true;
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
                                        server.reset_election_timeout = true;
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

                                    let (lock, timeout_cvar) = &*thread_pair;
                                    let mut server = lock.lock().unwrap();

                                    if server.apply_client_request(message) {
                                        // Client request received, replicate
                                        server.client_request_received = true;
                                        timeout_cvar.notify_one();
                                    }
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
    client_request_received: bool,

    state: ElectionState,
    current_votes: HashSet<String>,

    current_term: usize,
    voted_for: Option<String>,
    commit_index: usize,
    last_applied: usize,

    next_index: HashMap<String, usize>,
    match_index: HashMap<String, usize>,

    log: Vec<LogEntry>,
}

impl Server {
    /// Create a new server
    fn new(config: Config) -> Server {

        // Initialize indices of each other node to 0
        let zeros_next = vec![1; config.cluster.others.len()];
        let zeros_match = vec![0; config.cluster.others.len()];
        let next_index = config.cluster.others.iter().map(|o| o.addr.to_string()).zip(zeros_next).collect();
        let match_index = config.cluster.others.iter().map(|o| o.addr.to_string()).zip(zeros_match).collect();

        Server {
            config,
            leader_id: None,
            reset_election_timeout: false,
            client_request_received: false,
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

    /// Returns `true` if the message was accepted.
    ///
    /// # Arguments
    ///
    /// * `request` - An append entries request from the leader
    ///
    fn apply_append_entries_request(&mut self, mut request: AppendEntriesRequest) -> bool {

        let accepted = match self.state {
            ElectionState::Follower => {
                if request.term < self.current_term {
                    // Bad term
                    return false;
                }

                //let (my_prev_index, my_prev_term) = match self.log.las { }
                // TODO: check validity
                self.current_term = request.term;
                self.log.append(request.entries.as_mut());
                // TODO: remove this
                debug!("----- FOLLOWER NEW LOG");
                for e in self.log.iter() {
                    debug!("{:?}", e);
                }
                true
            },
            ElectionState::Candidate => {
                // TODO: check validity
                // If request.term > current_term then convert to follower
                true
            },
            ElectionState::Leader => {
                // TODO: check validity
                // If request.term > current_term then convert to follower
                true
            }
        };

        if accepted {
            self.voted_for = None;
            self.leader_id = Some(request.leader_id);
            // TODO: clear other fields
        }

        accepted
    }

    fn apply_append_entries_response(&mut self, _response: AppendEntriesResponse) {
        unimplemented!();
    }

    /// Returns `true` if the vote request was accepted and a vote was send.
    ///
    /// # Arguments
    ///
    /// * `request` - A vote request from a candidate
    ///
    fn apply_vote_request(&mut self, request: VoteRequest) -> bool{

        // Only vote as a follower
        if let ElectionState::Follower = self.state {

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
                    votee = Some(Member { addr: other.addr.clone() });
                    break;
                }
            }

            if let Some(m) = votee {

                // TODO: check eligibility
                let vote_request_accepted = true;

                if vote_request_accepted {
                    info!("Vote accepted: {:?}", request);
                    self.voted_for = Some(request.candidate_id.clone());

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
                } else {
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
            } else {
                // Candidate not found
                error!("Member not found for sending vote: {:?}", request.candidate_id);
                false
            }
        } else {
        //Candidate or leader
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
            // update term
        }
    }

    /// Returns `true` if valid client request was received and must be replicated.
    ///
    /// # Arguments
    ///
    /// * `request` - A request from a client
    ///
    fn apply_client_request(&mut self, message: ClientRequest) -> bool {

        match self.state {
            ElectionState::Leader => {
                // Handle message
                self.append_log_entries(vec![message.entry.clone()]);
                let result = query::Result::new(query::QUERY_RESULT_SUCCESS, "success".to_string(), "".to_string());
                send_client_response(self.id(), message, result);
                true
            },
            ElectionState::Follower => {
                if let Some(leader) = self.leader_id.borrow() {
                    // Redirect to leader by sending its address
                    let result = query::Result::new(query::QUERY_RESULT_REDIRECT, "leader redirect".to_string(), leader.to_string());
                    send_client_response(self.id(), message, result);
                    false
                }
                else {
                    // Currently no leader to handle the request
                    let result = query::Result::new(query::QUERY_RESULT_RETRY, "leader unknown".to_string(), "".to_string());
                    send_client_response(self.id(), message, result);
                    false
                }
            }
            ElectionState::Candidate => {
                // Leader offline, proceeding with election
                let result = query::Result::new(query::QUERY_RESULT_CANDIDATE, "leader offline".to_string(), "".to_string());
                send_client_response(self.id(), message, result);
                false
            }
        }
    }

    fn append_log_entries(&mut self, queries: Vec<Query>) {

        for q in queries {
            info!("will append log: {:?}", q);
            self.log.push(LogEntry {
                term: self.current_term,
                index: self.log.len() as usize + 1,
                data: q,
            });
            // TODO: apply to state machine
            self.last_applied +=1;
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
                let mut server = lock.lock().unwrap();

                // Send initial heartbeat
                let mut initial_append = HashMap::new();
                for (address, _next_index) in server.next_index.iter() {
                    initial_append.insert(address.clone(), Vec::new());
                }

                build_and_send_entries_async("initial append entries".into(), &server, initial_append);

                // Set maximum inactivity from leader to a percentage of the election timeout
                // TODO: fine tune
                let pct: f64 = 0.6;
                let max_inactivity = (server.config.election_timeout as f64 * pct) as u64;

                let result = timeout_cvar.wait_timeout(server, Duration::from_millis(max_inactivity)).unwrap();

                server = result.0;

                // Client request received, woken up for replication
                if server.client_request_received {
                    server.client_request_received = false;
                }

                // The new entries for each member
                let mut entries_map = HashMap::new();
                // The index increments i.e. the size of the new entries for each member
                let mut index_updates = HashMap::new();

                // For each member build entries to be sent and increments to index
                for (address, next_index) in server.next_index.iter() {

                    let from = *next_index - 1;
                    let to = server.log.len();

                    // Entries that will be send for that member
                    let mut entries: Vec<LogEntry> = Vec::new();

                    debug!("member:{}, from:{}, to:{}", address, from, to);

                    for i in from..to {
                        entries.push(server.log[i].clone());
                    }
                    index_updates.insert(address.clone(), entries.len());
                    entries_map.insert(address.clone(), entries);

                }

                build_and_send_entries_async("replicate log".into(), &server, entries_map);

                // Update the indices of next entry
                for (address, next_index) in server.next_index.iter_mut() {
                    *next_index += match index_updates.get(address) {
                        Some(i) => *i,
                        None => 0,
                    };
                }
                // TODO: remove this
                debug!("***** LEADER NEW LOG");
                for e in server.log.iter() {
                    debug!("{:?}", e);
                }
                debug!("next indices after update: {:?}", server.next_index);
            }
        })
        .unwrap();
}
fn build_and_send_entries_async(name: String, server: &Server, mut entries_map: HashMap<String, Vec<LogEntry>>) {

    for (address, next_index) in server.next_index.iter() {

        // Minus 1 as arrays are zero-based and Raft first message is at index 1
        let last_sent = if *next_index > 1 {
            *next_index - 2
        } else {
            0
        };
        let recipient = address.clone();
        let term = server.current_term;
        let leader_id = server.id();
        let (prev_log_index, prev_log_term) = match server.log.get(last_sent) {
            Some(e) => (e.index, e.term),
            None => (0, 0),
        };
        let leader_commit = server.commit_index;
        let entries = match entries_map.remove(address) {
            Some(e) => e,
            None => Vec::new(),
        };

        thread::Builder::new().name(name.clone()).spawn(move ||{
            message::send_append_entries(AppendEntriesRequest{
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            }, recipient);
        }).unwrap();
    }
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