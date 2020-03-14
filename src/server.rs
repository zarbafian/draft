use std::collections::{HashMap, HashSet};
use std::net::{UdpSocket};
use std::sync::{Arc, Mutex, Condvar};
use std::thread;
use std::borrow::Borrow;
use log::{trace, debug, info, warn, error};

use crate::net;
use crate::config::{Member, Config};
use crate::message::{self, AppendEntriesRequest, VoteRequest, LogEntry, VoteResponse, AppendEntriesResponse, ClientRequest, ClientResponse, Query, QueryResult};
use crate::behavior;
use crate::util::ThreadPool;
use crate::data::{self, StateMachine};

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

    let threads = config.handler_threads;
    let server = Server::new(config);
    info!("{:?}", server);

    // Create a thread pool for handling messages
    let pool = ThreadPool::new(threads);

    // Bind listener socket
    let socket = UdpSocket::bind(my_address)
        .expect("could not bind listener socket");

    // Receive buffer
    let mut buf = [0u8; 65535];

    // Shared data: server state machine + condition variable for message notification
    let shared_pair = Arc::new((Mutex::new(server), Condvar::new()));

    // Start server behavior
    behavior::start_consensus_behavior(shared_pair.clone());

    loop {
        if let Ok((amount, src)) = socket.recv_from(&mut buf) {

            // Messages should have at least two bytes
            if amount > 1 {

                // Copy data from buffer so the buffer can be used for another request
                let mut data = [0u8; 65535];
                data.copy_from_slice(&buf[..]);

                let thread_pair = shared_pair.clone();

                // Handling message
                pool.execute(
                    Box::new(move || {
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

                                    let (lock, timeout_cvar) = &*thread_pair;
                                    let mut server = lock.lock().unwrap();

                                    if server.apply_append_entries_response(message) {
                                        // Received higher term, notify leader behavior
                                        server.convert_to_follower = true;
                                        timeout_cvar.notify_one();
                                    }
                                },
                                message::Type::VoteRequest => {
                                    let message: VoteRequest = parse_json!(&json);
                                    debug!("Received message: {:?}", message);

                                    // Handle vote message
                                    let (lock, timeout_cvar) = &*thread_pair;
                                    let mut server = lock.lock().unwrap();

                                    if server.apply_vote_request(message) {
                                        // Sent a vote, reset election timeout
                                        server.reset_election_timeout = true;
                                        timeout_cvar.notify_one();
                                    }
                                },
                                message::Type::VoteResponse => {
                                    let message: VoteResponse = parse_json!(&json);
                                    debug!("Received message: {:?}", message);

                                    let (lock, timeout_cvar) = &*thread_pair;
                                    let mut server = lock.lock().unwrap();

                                    if server.apply_vote_response(message) {
                                        // Convert to follower
                                        server.convert_to_follower = true;
                                    }
                                    // Notify anyway to check if candidate was elected
                                    timeout_cvar.notify_one();
                                }
                                message::Type::ClientRequest => {
                                    let message: ClientRequest = parse_json!(&json);
                                    debug!("Received message: {:?}", message);

                                    let (lock, timeout_cvar) = &*thread_pair;
                                    let mut server = lock.lock().unwrap();

                                    let client = message.client_id.clone();

                                    // `Some` is returned if the entries were added, `None` otherwise (i.e. read only or error with client queries)
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
                    })
                );
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
pub struct Server {
    pub reset_election_timeout: bool,
    pub client_request_received: bool,
    pub convert_to_follower: bool,

    config: Config,
    leader_id: Option<String>,

    state: ElectionState,
    current_votes: HashSet<String>,

    current_term: usize,
    voted_for: Option<String>,
    commit_index: usize,
    last_applied: usize,

    next_index: HashMap<String, usize>,
    match_index: HashMap<String, usize>,

    log: Vec<LogEntry>,
    state_machine: StateMachine,

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
            convert_to_follower: false,
            state: ElectionState::Follower,
            current_votes: HashSet::new(),
            current_term: 0,
            voted_for: None,
            commit_index: 0,
            last_applied: 0,
            next_index,
            match_index,
            log: Vec::new(),
            state_machine: StateMachine::new(),
            pending_responses: HashMap::new(),
        }
    }

    /// Returns the identifier of the server.
    pub fn id(&self) -> String {
        return self.config.cluster.me.addr.to_string();
    }

    /// Returns `true` if the server has received a majority of the votes.
    pub fn was_elected(&self) -> bool {
        self.current_votes.len() as f64 > ((self.config.cluster.others.len() + 1) as f64 / 2 as f64)
    }

    /// Returns the term and index of the last log entry.
    pub fn last_entry_indices(&self) -> (usize, usize) {
        match self.log.last() {
            Some(e) => (e.term, e.index),
            None => (0,0)
        }
    }

    /// Returns the election timeout in milliseconds.
    pub fn get_election_timeout(&self) -> u64 {
        self.config.election_timeout
    }

    /// Returns the election randomness in milliseconds.
    pub fn get_election_randomness(&self) -> u64 {
        self.config.election_randomness
    }

    /// Returns the maximum leader inactivity in milliseconds.
    pub fn get_max_inactivity(&self) -> u64 {
        self.config.max_inactivity
    }

    /// Returns the current term.
    pub fn get_current_term(&self) -> usize {
        self.current_term
    }

    /// Returns a list of all the other members of the cluster.
    pub fn get_other_servers(&self) -> Vec<Member> {
        let mut others: Vec<Member> = Vec::new();
        self.config.cluster.others.iter().for_each(|m| others.push(m.clone()));
        others
    }

    /// Update commit and apply commited entries to state machine
    fn update_state_machine(&mut self, commit_index: usize) {
        let commit_index_start = self.commit_index; // array index of last committed index, plus one
        let commit_index_end = commit_index; // array index right after new commit index
        for entry in &self.log[commit_index_start..commit_index_end] {
            self.state_machine.execute_query(entry.data.clone());
        }

        // Update last applied
        self.last_applied = commit_index;

        // Update commit index
        self.commit_index = commit_index;
    }

    /// Returns the initial empty append entries to be sent to the followers.
    ///
    /// # Return
    ///
    /// A map where the `key` is the id of the member and `value` is an empty vector.
    pub fn get_initial_entries(&self) -> HashMap<Member, AppendEntriesRequest> {
        let (prev_log_term, prev_log_index) = self.last_entry_indices();

        let mut map = HashMap::new();

        for member in self.get_other_servers() {
            map.insert(member.clone(), AppendEntriesRequest{
                term: self.current_term,
                leader_id: self.id(),
                prev_log_index,
                prev_log_term,
                entries: Vec::new(),
                leader_commit: self.commit_index
            });
        }
        map
    }

    /// Returns the list of next log entries to be sent to the followers.
    ///
    /// # Return
    ///
    /// A map where the `key` is the id of the member and `value` are its log entries.
    pub fn get_next_entries(&self) -> HashMap<Member, AppendEntriesRequest> {
        let mut map = HashMap::new();

        for member in self.get_other_servers() {
            // Retrieve next index of member
            let next_index = match self.next_index.get(&member.addr.to_string()) {
                Some(i) => i,
                None => &1
            };

            // Previous indices
            let (prev_log_term, prev_log_index) = if *next_index > 1 {
                // Minus 1 to get the previous index, minus two as array is zero-based
                match self.log.get(*next_index - 2) {
                    Some(e) => (e.term, e.index),
                    None => (0, 0),
                }
            }
            else {
                (0, 0)
            };

            // Indices in log array
            let from = *next_index - 1;
            let to = self.log.len();

            // Entries that will be send for that member
            let mut entries: Vec<LogEntry> = Vec::new();

            for i in from..to {
                entries.push(self.log[i].clone());
            }

            map.insert(member.clone(), AppendEntriesRequest{
                term: self.current_term,
                leader_id: self.id(),
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index
            });
        }
        map
    }

    /// Update state to become a follower.
    pub fn init_follower(&mut self) {
        self.state = ElectionState::Follower;
        self.voted_for = None;
        self.current_votes.clear();

        self.reset_election_timeout = false;
        self.client_request_received = false;
        self.convert_to_follower = false;
        self.pending_responses.clear();
    }

    /// Update state to start a new election.
    pub fn init_election(&mut self) {
        self.state = ElectionState::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id());
        self.current_votes.clear();
        self.current_votes.insert(self.id());
        self.leader_id = None;

        self.reset_election_timeout = false;
        self.client_request_received = false;
        self.convert_to_follower = false;
        self.pending_responses.clear();
    }

    /// Update state to become a leader.
    pub fn init_leader(&mut self) {
        self.state = ElectionState::Leader;
        self.voted_for = None;
        self.current_votes.clear();
        self.leader_id = Some(self.id());

        self.reset_election_timeout = false;
        self.client_request_received = false;
        self.convert_to_follower = false;
        self.pending_responses.clear();

        // Value of index right after the last log
        let (_, last_index) = self.last_entry_indices();

        // Initialize next_index
        for index in self.next_index.values_mut() {
            *index = last_index + 1;
        }

        // Initialize match_index
        for index in self.match_index.values_mut() {
            *index = 0;
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
                net::send_append_entries_response(
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

                    self.log.clear();

                    // Append new entries
                    let entries_count = request.entries.len();
                    self.log.append(request.entries.as_mut());

                    // Reply true
                    let term = self.current_term;
                    thread::spawn(move ||{
                        net::send_append_entries_response(
                            AppendEntriesResponse{
                                sender_id,
                                term,
                                success: true,
                                last_index: entries_count
                            },
                            recipient
                        );
                    });

                    if request.leader_commit > self.commit_index {
                        self.update_state_machine(request.leader_commit);
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

                                let new_entries_count = request.entries.len();

                                // Append new entries
                                self.log.append(request.entries.as_mut());

                                // Reply true
                                let term = self.current_term;
                                let last_index = match self.log.last() {
                                    Some(e) => e.index,
                                    None => 0,
                                };

                                thread::spawn(move || {

                                    net::send_append_entries_response(
                                        AppendEntriesResponse {
                                            sender_id,
                                            term,
                                            success: true,
                                            last_index
                                        },
                                        recipient
                                    );
                                });

                                // Update commit
                                if request.leader_commit > self.commit_index {
                                    self.update_state_machine(request.leader_commit);
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
                                    net::send_append_entries_response(
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
                            debug!("I am late with only {} entries", self.log.len());
                            let term = self.current_term;
                            thread::spawn(move || {
                                net::send_append_entries_response(
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

    /// Returns `true` if server should convert to follower.
    ///
    /// # Arguments
    ///
    /// * `response` - An append entries response from a follower
    ///
    fn apply_append_entries_response(&mut self, response: AppendEntriesResponse) -> bool {

        if response.term > self.current_term {
            self.current_term = response.term;
            return true;
        }

        let sender_id = response.sender_id.clone();

        let next_index = match self.next_index.get_mut(&sender_id) {
            Some(i) => i,
            None => {
                warn!("Received response from unknown member: {}", sender_id);
                return false;
            }
        };

        let match_index = match self.match_index.get_mut(&response.sender_id) {
            Some(i) => i,
            None => {
                error!("No match_index entry for member: {}", sender_id);
                return false;
            }
        };

        if response.success {
            // Update next and match index
            *next_index = response.last_index + 1;
            *match_index = response.last_index + 1;

            // Update of commit index
            let mut sorted_indices = Vec::new();

            // Gather list of indices for other members
            self.next_index.iter().for_each(|(_, &value)| { sorted_indices.push(value) });

            // Add self
            let (last_term, last_index) = self.last_entry_indices();
            sorted_indices.push(last_index + 1);

            // Sort in ascending order, and reverse to have an descending order
            sorted_indices.sort();
            sorted_indices.reverse();

            // The commit index is the highest index replicated on a majority
            let required_majority = ((self.next_index.len() + 1) as f64 / 2 as f64).ceil() as usize;
            let commit_index = sorted_indices[required_majority - 1] - 1;

            if commit_index > self.commit_index && last_term == self.current_term {
                debug!("Commit index, sorted_indices={:?}, required_majority={}, commit_index={} -> commit_index={}", sorted_indices, required_majority, self.commit_index, commit_index);

                // Apply to state machine
                self.update_state_machine(commit_index);
            }

            // Respond to pending request from client
            let mut answered_responses = Vec::new();
            for (client_id, (request, required_commit)) in self.pending_responses.iter() {
                if self.commit_index >= *required_commit {
                    // Respond to client
                    let result = QueryResult::new(data::QUERY_RESULT_SUCCESS, "success".to_string(), "".to_string());
                    send_client_response(self.id(), request.clone(), result);
                    answered_responses.push(client_id.clone());
                }
            }

            // Clear pending requests
            for key in answered_responses.iter() {
                self.pending_responses.remove(key);
            }
            debug!("Size of pending requests: {}", self.pending_responses.len());
        }
        else {
            *next_index -= 1;
        }
        false
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
                info!("Vote rejected because candidate is behind: {}, {} < {}, {}", request.last_log_term, request.last_log_index, my_last_term, my_last_index);
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

    /// Returns `true` if server should convert to follower.
    ///
    /// # Arguments
    ///
    /// * `response` - A vote response from a follower
    ///
    fn apply_vote_response(&mut self, response: VoteResponse) -> bool {
        // Check term of response
        if response.term > self.current_term {
            self.current_term = response.term;
            return true;
        }

        // Check response
        if response.vote_granted {
            self.current_votes.insert(response.voter_id);
        }

        false
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
                match message.query.action {
                    message::Action::Get => {
                        // Handle read only query
                        let result = self.state_machine.execute_query(message.query.clone());
                        send_client_response(self.id(), message, result);
                        None
                    }
                    _ => {
                        // Handle write query
                        let log_index = self.append_log_entry(message.query);
                        Some(log_index)
                    }
                }
            },
            ElectionState::Follower => {
                if let Some(leader) = self.leader_id.borrow() {
                    // Redirect to leader by sending its address
                    let result = QueryResult::new(data::QUERY_RESULT_REDIRECT, "leader redirect".to_string(), leader.to_string());
                    send_client_response(self.id(), message, result);
                    None
                }
                else {
                    // Currently no leader to handle the request
                    let result = QueryResult::new(data::QUERY_RESULT_RETRY, "leader unknown".to_string(), "".to_string());
                    send_client_response(self.id(), message, result);
                    None
                }
            }
            ElectionState::Candidate => {
                // Leader offline, proceeding with election
                let result = QueryResult::new(data::QUERY_RESULT_CANDIDATE, "leader offline".to_string(), "".to_string());
                send_client_response(self.id(), message, result);
                None
            }
        }
    }

    /// Returns the commit index for the entries to be considered commited
    fn append_log_entry(&mut self, query: Query) -> usize {

        info!("will append log: {:?}", query);
        self.log.push(LogEntry {
            term: self.current_term,
            index: self.log.len() + 1,
            data: query,
        });

        self.log.len()
    }
}

fn send_vote_response(response: VoteResponse, recipient: String) {

    thread::Builder::new()
        .name("vote response".into())
        .spawn(move || {
            // Send vote
            net::send_vote_response(response, recipient);
        })
        .unwrap();
}

fn send_client_response(server_id: String, request: ClientRequest, result: QueryResult) {
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
            net::send_client_response(response, request.client_id);
        })
        .unwrap();
}
