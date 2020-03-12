use std::collections::{HashMap, HashSet};
use std::net::{UdpSocket};
use std::sync::{Arc, Mutex, Condvar};
use std::thread;
use rand::Rng;
use std::time::{Duration, Instant};
use std::borrow::Borrow;
use log::{trace, debug, info, warn, error};

use crate::query;
use crate::config::{Config};
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

    // Shared data: server state machine + condition variable for message notification
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

                                    let client = message.client_id.clone();
                                    if let Some(i) = server.apply_client_request(message.clone()) {
                                        // Client request received, replicate
                                        server.pending_responses.insert(client, (message, i));
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

    pending_responses: HashMap<String, (ClientRequest, usize)>,
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
            pending_responses: HashMap::new(),
        }
    }

    fn id(&self) -> String {
        return self.config.cluster.me.addr.to_string();
    }

    fn was_elected(&self) -> bool {
        self.current_votes.len() as f64 > ((self.config.cluster.others.len() + 1) as f64 / 2 as f64)
    }

    fn last_entry_indices(&self) -> (usize, usize) {
        match self.log.last() {
            Some(e) => (e.term, e.index),
            None => (0,0)
        }
    }

    /// Returns `true` if the message was accepted.
    ///
    /// # Arguments
    ///
    /// * `request` - An append entries request from the leader
    ///
    fn apply_append_entries_request(&mut self, mut request: AppendEntriesRequest) -> bool {

        // Same term checking in all states
        if request.term < self.current_term {
            error!("Received invalid term {}, current is {}", request.term, self.current_term);
            let term = self.current_term;
            let recipient = request.leader_id.clone();
            let sender_id = self.id();
            // Bad term
            thread::spawn(move ||{
                message::send_append_entries_response(
                    AppendEntriesResponse{
                        sender_id,
                        term,
                        success: false,
                        last_index: 0
                    },
                    recipient
                );
            });
            return false;
        }

        let accepted = match self.state {
            ElectionState::Follower => {

                // Update my term
                self.current_term = request.term;

                // Address of leader
                let recipient = request.leader_id.clone();
                let sender_id = self.id();

                // Start scenario
                if request.prev_log_index == 0 {

                    info!("Received first log");
                    self.log.clear();

                    // Append new entries
                    let entries_count = request.entries.len();
                    self.log.append(request.entries.as_mut());

                    // Reply true
                    let term = self.current_term;
                    thread::spawn(move ||{
                        message::send_append_entries_response(
                            AppendEntriesResponse{
                                sender_id,
                                term,
                                success: true,
                                last_index: entries_count
                            },
                            recipient
                        );
                    });

                    // TODO: remove this
                    debug!("----- FOLLOWER NEW LOG");
                    for e in self.log.iter() {
                        debug!("{:?}", e);
                    }
                }
                // Normal scenario
                else {
                    // Array index of previous log
                    let prev_log_array_index = request.prev_log_index - 1;

                    let my_entry = self.log.get(prev_log_array_index);

                    debug!("request.prev_log_index={}, entry={:?}", request.prev_log_index, my_entry);

                    match my_entry {
                        Some(entry) => {

                            // Found, compare term
                            if entry.term == request.prev_log_term {
                                // Last entry match, remove everything after (normally nothing)
                                debug!("Previous index and term matched");
                                if self.log.len() > request.prev_log_index {
                                    warn!("Received duplicate entries: self.log.len()={}, request.prev_log_index={}", self.log.len(), request.prev_log_index);
                                    let first_to_delete = request.prev_log_index; // -1 for array indexing, +1 for right after
                                    let last_to_delete = self.log.len();
                                    for _i in first_to_delete..last_to_delete {
                                        self.log.pop();
                                    }
                                }
                                // Append new entries
                                self.log.append(request.entries.as_mut());

                                // Reply true
                                let term = self.current_term;
                                let last_index = match self.log.last() {
                                    Some(e) => e.index,
                                    None => 0,
                                };

                                thread::spawn(move || {

                                    message::send_append_entries_response(
                                        AppendEntriesResponse {
                                            sender_id,
                                            term,
                                            success: true,
                                            last_index
                                        },
                                        recipient
                                    );
                                });

                                // TODO: remove this
                                debug!("----- FOLLOWER NEW LOG");
                                for e in self.log.iter() {
                                    debug!("{:?}", e);
                                }

                            } else {
                                debug!("Previous index and term did not match, delete entries after ");
                                let first_to_delete = request.prev_log_index - 1; // -1 for array indexing
                                let last_to_delete = self.log.len();
                                for _i in first_to_delete..last_to_delete {
                                    self.log.pop();
                                }

                                // Respond false so leader decreases its next_index
                                let term = self.current_term;
                                thread::spawn(move || {
                                    message::send_append_entries_response(
                                        AppendEntriesResponse {
                                            sender_id,
                                            term,
                                            success: false,
                                            last_index: 0
                                        },
                                        recipient
                                    );
                                });
                            }
                        },
                        None => {
                            // Respond false so leader decreases its next_index
                            debug!("I am back with only {} entries", self.log.len());
                            let term = self.current_term;
                            thread::spawn(move || {
                                message::send_append_entries_response(
                                    AppendEntriesResponse {
                                        sender_id,
                                        term,
                                        success: false,
                                        last_index: 0
                                    },
                                    recipient
                                );
                            });
                        }
                    }
                }
                true // To reset election timeout
            },
            ElectionState::Candidate => {
                // Convert to follower
                true
            },
            ElectionState::Leader => {
                // Convert to follower
                true
            }
        };

        if accepted {
            self.voted_for = None;
            self.leader_id = Some(request.leader_id);
            self.current_votes.clear();
        }

        accepted
    }

    fn apply_append_entries_response(&mut self, response: AppendEntriesResponse) {

        let sender_id = response.sender_id.clone();

        let next_index = match self.next_index.get_mut(&sender_id) {
            Some(i) => i,
            None => {
                warn!("Received response from unknown member: {}", sender_id);
                return;
            }
        };

        let match_index = match self.match_index.get_mut(&response.sender_id) {
            Some(i) => i,
            None => {
                error!("No match_index entry for member: {}", sender_id);
                return;
            }
        };

        if response.success {
            // Update next and match index
            *next_index = response.last_index + 1;
            *match_index = response.last_index + 1;

            // Update of commit index
            let mut sorted_indices = Vec::new();

            // Gather list of all indices
            self.next_index.iter().for_each(|(_, &value)| { sorted_indices.push(value) });

            // Sort in ascending order, and reverse to have an descending order
            sorted_indices.sort();
            sorted_indices.reverse();

            // The commit index is the highest index replicated on a majority
            let required_majority = (self.next_index.len() as f64 / 2 as f64).ceil() as usize;
            let commit_index = sorted_indices[required_majority - 1] - 1;

            // Update commit index
            self.commit_index = commit_index;

            // Respond to pending request from client
            let mut answered_responses = Vec::new();
            for (client_id, (request, required_commit)) in self.pending_responses.iter() {
                if self.commit_index >= *required_commit {
                    // Respond to client
                    let result = query::Result::new(query::QUERY_RESULT_SUCCESS, "success".to_string(), "".to_string());
                    send_client_response(self.id(), request.clone(), result);
                    answered_responses.push(client_id.clone());
                }
            }

            // Clear pending requests
            for key in answered_responses.iter() {
                self.pending_responses.remove(key);
            }
            info!("Size of pending requests: {}", self.pending_responses.len());
        }
        else {
            *next_index -= 1;
        }
    }

    /// Returns `true` if the vote request was accepted and a vote was send.
    ///
    /// # Arguments
    ///
    /// * `request` - A vote request from a candidate
    ///
    fn apply_vote_request(&mut self, request: VoteRequest) -> bool {

        // Only vote as a follower
        if let ElectionState::Follower = self.state {

            let (my_last_term, my_last_index) = self.last_entry_indices();

            if request.last_log_term < my_last_term || (request.last_log_term == my_last_term && request.last_log_index < my_last_index) {
                // Vote rejected because candidate is behind
                info!("Vote rejected because candidate is behin: {}, {} < {}, {}", request.last_log_term, request.last_log_index, my_last_term, my_last_index);
                send_vote_response(VoteResponse {
                    voter_id: self.id(),
                    term: self.current_term,
                    vote_granted: false,
                }, request.candidate_id);
                return false;
            }

            if let Some(votee) = self.voted_for.borrow() {
                if !votee.eq(&request.candidate_id) {
                    // Already voted for someone else
                    info!("already voted for someone: {}", votee);
                    return false;
                }
            }


            // Vote accepted
            info!("Vote accepted: {:?}", request);
            self.voted_for = Some(request.candidate_id.clone());

            send_vote_response(VoteResponse {
                    voter_id: self.id(),
                    term: self.current_term,
                    vote_granted: true,
                }, request.candidate_id);
            // Vote sent
            true
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
            if response.term > self.current_term {
                self.current_term = response.term;
                // TODO: switch to follower
            }
        }
    }

    /// Returns `true` if valid client request was received and must be replicated.
    ///
    /// # Arguments
    ///
    /// * `request` - A request from a client
    ///
    fn apply_client_request(&mut self, message: ClientRequest) -> Option<usize> {

        match self.state {
            ElectionState::Leader => {
                // Handle message
                let log_index = self.append_log_entries(vec![message.entry.clone()]);
                Some(log_index)
                /*
                if self.append_log_entries(vec![message.entry.clone()]) {
                    //let result = query::Result::new(query::QUERY_RESULT_SUCCESS, "success".to_string(), "".to_string());
                    //send_client_response(self.id(), message, result);
                    true
                }
                else {
                    let result = query::Result::new(query::QUERY_RESULT_REJECTED, "request rejected".to_string(), "could not apply request to the state machine".to_string());
                    send_client_response(self.id(), message, result);
                    false
                }
                */
            },
            ElectionState::Follower => {
                if let Some(leader) = self.leader_id.borrow() {
                    // Redirect to leader by sending its address
                    let result = query::Result::new(query::QUERY_RESULT_REDIRECT, "leader redirect".to_string(), leader.to_string());
                    send_client_response(self.id(), message, result);
                    None
                }
                else {
                    // Currently no leader to handle the request
                    let result = query::Result::new(query::QUERY_RESULT_RETRY, "leader unknown".to_string(), "".to_string());
                    send_client_response(self.id(), message, result);
                    None
                }
            }
            ElectionState::Candidate => {
                // Leader offline, proceeding with election
                let result = query::Result::new(query::QUERY_RESULT_CANDIDATE, "leader offline".to_string(), "".to_string());
                send_client_response(self.id(), message, result);
                None
            }
        }
    }

    /// Returns the commit index for the entries to be considered commited
    fn append_log_entries(&mut self, queries: Vec<Query>) -> usize {

        for q in queries {
            info!("will append log: {:?}", q);
            self.log.push(LogEntry {
                term: self.current_term,
                index: self.log.len() + 1,
                data: q,
            });
        }

        self.log.len()
    }
}

fn send_vote_response(response: VoteResponse, recipient: String) {

    thread::Builder::new()
        .name("vote response".into())
        .spawn(move || {
            // Send vote
            message::send_vote_response(response, recipient);
        })
        .unwrap();
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

    server.reset_election_timeout = false;
    server.client_request_received = false;
    server.state = ElectionState::Follower;
    server.voted_for = None;
    server.current_votes.clear();

    start_follower_timeout_thread(timeout_pair.clone());
}

fn handle_election_won(timeout_pair: Arc<(Mutex<Server>, Condvar)>) {
    info!("---------------------------------------------");
    info!("------------ handle_election_won ------------");
    info!("---------------------------------------------");

    let (lock, _timeout_cvar) = &*timeout_pair;
    let mut server = lock.lock().unwrap();

    // Update state
    server.leader_id = Some(server.id());
    server.reset_election_timeout = false;
    server.client_request_received = false;
    server.state = ElectionState::Leader;
    server.voted_for = None;
    server.current_votes.clear();

    // Value of index right after the last log
    let next_entry = match server.log.last() {
        Some(l) => l.index + 1,
        None => 1,
    };

    // Initialize next_index to the
    for index in server.next_index.values_mut() {
        *index = next_entry;
    }

    start_leader_thread(timeout_pair.clone());
}

fn start_leader_thread(timeout_pair: Arc<(Mutex<Server>, Condvar)>) {
    thread::Builder::new()
        .name(String::from("leader"))
        .spawn(move || {
            info!("started");

            {
                let (lock, _timeout_cvar) = &*timeout_pair.clone();
                let server = lock.lock().unwrap();

                // Send initial heartbeat
                let mut initial_append = HashMap::new();
                for (address, _next_index) in server.next_index.iter() {
                    initial_append.insert(address.clone(), Vec::new());
                }

                build_and_send_entries_async("initial append entries".into(), &server, initial_append);
            }

            loop {
                let (lock, timeout_cvar) = &*timeout_pair.clone();
                let mut server = lock.lock().unwrap();

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
                    entries_map.insert(address.clone(), entries);

                }

                build_and_send_entries_async("replicate log".into(), &server, entries_map);

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

        // At least one log entry was previously sent
        let has_sent = *next_index > 1;

        let recipient = address.clone();
        let term = server.current_term;
        let leader_id = server.id();

        let (prev_log_index, prev_log_term) = if has_sent {
            // Minus 1 as arrays are zero-based and Raft first message is at index 1
            match server.log.get(*next_index - 2) {
                Some(e) => (e.index, e.term),
                None => (0, 0),
            }
        }
        else {
            (0, 0)
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