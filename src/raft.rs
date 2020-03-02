use std::thread::{self, JoinHandle};
use std::net::UdpSocket;

use crate::message::{self, AppendEntriesRequest, VoteRequest, VoteResponse};
use crate::config::Config;
use std::time::{Duration, Instant};
use std::sync::{Arc, Mutex, Condvar};
use rand::Rng;

// Requirements for safety:
//  - append_xxx_message cannot try to acquire lock as they are called inside a lock
//  - behaviors check that status hasn't changed before starting (TODO: fixme)

macro_rules! parse_json {
    ($x:expr) => {
        match serde_json::from_str($x) {
            Ok(r) => r,
            Err(err) => {
                eprintln!("Request could not be parsed: {}", err);
                return;
            },
        }
    };
}

pub fn run(config: Config) {

    // Copy my address before it is moved
    let my_address = config.cluster.me.addr.clone();

    let state_machine = StateMachine::new(config);

    // Bind listener socket
    let socket = UdpSocket::bind(my_address)
        .expect("could not bind listener socket");

    // Receive buffer
    let mut buf = [0u8; 65535];

    // Shared data : state machine + condition variable for hearbeat received + condition variable for won election
    let original_pair = Arc::new((Mutex::new(state_machine), Condvar::new(), Condvar::new()));
    // Clone that is moved to the behavior thread
    let behavior_pair = original_pair.clone();

    // Start consensus
    start_behavior(behavior_pair);

    loop {
        // Clone at each loop so it can move in the thread closure
        let moved_loop_pair = original_pair.clone();

        if let Ok((amt, src)) = socket.recv_from(&mut buf) {

            // Messages should have at least two bytes
            if amt > 1 {

                // Retrieve data from buffer so it can be used for another request
                let mut data = [0u8; 65535];
                data.copy_from_slice(&buf[..]);

                // Message handling thread
                thread::spawn(move || {

                    println!("Received {} bytes from {:?}", amt, src);

                    // Check that we received a valid message type in the first byte
                    if let Some(message_type) = message::get_type(data[0]) {

                        let json: String = String::from_utf8_lossy(&data[1..amt]).to_string();

                        match message_type {
                            message::Type::AppendEntriesRequest => {
                                let message: AppendEntriesRequest = parse_json!(&json);
                                println!("Received message: {:?}", message);

                                let (lock, follower_cvar, _candidate_cvar) = &*moved_loop_pair;
                                let mut state_machine = lock.lock().unwrap();

                                state_machine.apply_append_entries_message(message);

                                // Notify of received heartbeat
                                follower_cvar.notify_one();
                            },
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
                            _ => ()
                        }

                    }
                });
            }
        }
    }
}

#[derive(Debug, Copy, Clone)]
enum ElectionState {
    Follower,
    Candidate,
    Leader
}

struct StateMachine {
    config: Config,

    current_state: ElectionState,
    heartbeat_received: bool,
    candidate_votes: u64,

    term: u64,
    last_log_index: u64,
    last_log_term: u64,
}

impl StateMachine {

    fn new(config: Config) -> StateMachine {
        StateMachine {
            config,
            current_state: ElectionState::Follower,
            term: 0,
            last_log_index: 0,
            last_log_term: 0,
            heartbeat_received: false,
            candidate_votes: 0,
        }
    }

    fn id(&self) -> String {
        self.config.cluster.me.addr.to_string()
    }

    fn has_won_election(&self) -> bool {
        self.candidate_votes >= ((self.config.cluster.others.len() + 1) / 2) as u64
    }

    fn apply_append_entries_message(&mut self, _message: AppendEntriesRequest) {

        // TODO: check message content
        self.heartbeat_received = true;
    }

    fn apply_vote_request_message(&mut self, message: VoteRequest) {

        // TODO: check vote validity

        let vote = VoteResponse {
            term: message.term,
            term_granted: true,
        };

        // Send vote
        message::send_vote(vote, &message.candidate_id, &self.config);
    }

    fn apply_vote_response_message(&mut self, _message: VoteResponse) {

        // TODO: check term of vote request and log index
        // TODO: check if a vote has not already been sent for this election
        self.candidate_votes += 1;
    }

    fn try_election_timeout(&mut self) -> bool {

        if self.heartbeat_received {

            // Reset flag and wait for another heartbeat
            self.heartbeat_received = false;

            // Has not timed out
            false
        } else {

            // Switch to candidate
            self.current_state = ElectionState::Candidate;

            // Has timed out
            true
        }
    }

    fn try_candidate_timeout(&mut self) -> bool {

        let result;

        if self.candidate_votes > ((self.config.cluster.others.len() + 1) / 2) as u64 {
            // Obtained majority (did not reach timeout)
            self.current_state = ElectionState::Leader;
            result = false;
        }
        else {
            // Timeout: did not win this round
            result = true;
        }

        // Reset votes
        self.candidate_votes = 0;

        result
    }
}

fn start_behavior(arc_pair: Arc<(Mutex<StateMachine>, Condvar, Condvar)>) {

    thread::spawn(move ||{

        // life of state machine: start a behavior and wait for it to finish
        loop {

            // Reference that is used by the behavior
            let moved_clone = arc_pair.clone();

            // Retrieve state to determine behavior
            let election_state;
            {
                let tmp_clone = arc_pair.clone();
                let (lock, _follower_cvar, _candidate_cvar) = &*tmp_clone;
                let state_machine = lock.lock().unwrap();
                election_state = state_machine.current_state;
            }

            // TODO: refactor as status could change between lock release and re-acquiring it in behavior

            // Start behavior
            let handle = match election_state {
                ElectionState::Follower => follower_behavior(moved_clone),
                ElectionState::Candidate => candidate_behavior(moved_clone),
                ElectionState::Leader => leader_behavior(moved_clone),
            };

            // Wait for behavior to finish
            handle.join().unwrap()
        }
    });
}

fn candidate_behavior(arc_pair: Arc<(Mutex<StateMachine>, Condvar, Condvar)>) -> JoinHandle<()> {

    println!("===== BEGIN candidate");

    thread::spawn(move || {

        let (lock, _follower_cvar, candidate_cvar) = &*arc_pair;

        // Loop until reaching a majority or a timeout
        loop {
            println!("== candidate: loop start");

            let mut state_machine = lock.lock().unwrap();

            // Check that state is still candidate
            if let ElectionState::Candidate = state_machine.current_state {
                // Start with a random wait
                let candidate_randomness = rand::thread_rng().gen_range(0, state_machine.config.candidate_randomness);
                thread::sleep(Duration::from_millis(candidate_randomness));

                // TODO: vote for self
                // TODO: update term on unsuccessful election
                // Send vote request
                let vote_request = VoteRequest {
                    candidate_id: state_machine.id(),
                    term: state_machine.term,
                    last_log_index: state_machine.last_log_index,
                    last_log_term: state_machine.last_log_term,
                };

                // Send vote requests to network
                message::broadcast(vote_request, &state_machine.config);

                println!("== candidate - wait for vote: {}", 0);

                let start = Instant::now();

                let timeout = state_machine.config.candidate_timout;
                let result = candidate_cvar.wait_timeout(state_machine, Duration::from_millis(timeout)).unwrap();

                let duration = start.elapsed();
                println!("== candidate - waited: {}", duration.as_millis());

                state_machine = result.0;

                if state_machine.try_candidate_timeout() {
                    println!("== candidate - timeout: I was not elected.");
                    continue;
                } else {
                    println!("== candidate: Victory - < == [Long Live The King] == >");
                    break;
                }
            }
            // State has changed
            else {
                break;
            }
        }

        println!("===== END candidate");
    })
}

fn leader_behavior(arc_pair: Arc<(Mutex<StateMachine>, Condvar, Condvar)>) -> JoinHandle<()> {

    println!("***** BEGIN leader");

    thread::spawn(move || {

        let (lock, _follower_cvar, _candidate_cvar) = &*arc_pair;

        loop {
            println!("** leader: loop start");

            let mut state_machine = lock.lock().unwrap();

            // Check that state is still leader
            if let ElectionState::Leader = state_machine.current_state {

                thread::sleep(Duration::from_secs(15));
            }
            // State has changed
            else {
                break;
            }
        }

        println!("***** END leader");
    })
}

fn follower_behavior(arc_pair: Arc<(Mutex<StateMachine>, Condvar, Condvar)>) -> JoinHandle<()> {

    println!("----- BEGIN follower");

    thread::spawn(move || {

        let (lock, follower_cvar, _candidate_cvar) = &*arc_pair;

        // Loop until an election timeout occurs
        loop {
            println!("-- follower: loop start");

            let mut state_machine = lock.lock().unwrap();

            // Check that state is still follower
            if let ElectionState::Follower = state_machine.current_state {
                println!("-- follower - wait for heartbeat: {}", 0);
                let start = Instant::now();

                let timeout = state_machine.config.election_timout;
                let result = follower_cvar.wait_timeout(state_machine, Duration::from_millis(timeout)).unwrap();

                // TODO: check value of timeout result in result.1

                let duration = start.elapsed();
                println!("-- follower - waited: {}", duration.as_millis());

                state_machine = result.0;

                if state_machine.try_election_timeout() {
                    println!("-- follower - timeout! Viva la revolucion!");
                    break;
                } else {
                    println!("-- follower: received HEARTBEAT -|v-|v-|v-|v-|v-|v-|v");
                    continue;
                }
            }
            // State has changed
            else {
                break;
            }
        }

        println!("----- END follower");
    })
}
