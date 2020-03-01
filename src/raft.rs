use std::thread::{self, JoinHandle};
use std::net::UdpSocket;

use crate::message::{self, AppendEntriesRequest};
use crate::config::Config;
use std::time::{Duration, Instant};
use std::sync::{Arc, Mutex, Condvar};

pub fn run(config: Config) {

    // Copy my address before it is moved
    let my_address = config.cluster.me.addr.clone();

    let state_machine = StateMachine::new(config);

    // Bind listener socket
    let socket = UdpSocket::bind(my_address)
        .expect("could not bind listener socket");

    // Receive buffer
    let mut buf = [0u8; 65535];

    // Condition variable for hearbeat received
    let original_pair = Arc::new((Mutex::new(state_machine), Condvar::new()));
    let behavior_pair = original_pair.clone();

    // Start consensus
    start_behavior(behavior_pair);

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

                                //state_machine.heartbeat_received = true;
                                state_machine.apply_message(request);

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

#[derive(Debug, Copy, Clone)]
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

    fn apply_message(&mut self, message: AppendEntriesRequest) {

        // TODO: check message content
        self.heartbeat_received = true;
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
}

fn start_behavior(arc_pair: Arc<(Mutex<StateMachine>, Condvar)>) {

    thread::spawn(move ||{

        // life of state machine: start a behavior and wait for it to finish
        loop {

            let moved_clone = arc_pair.clone();

            // Retrieve state to determine behavior
            let election_state;
            {
                let tmp_clone = arc_pair.clone();
                let (lock, _cvar) = &*tmp_clone;
                let state_machine = lock.lock().unwrap();
                election_state = state_machine.current_state;
            }

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

fn candidate_behavior(arc_pair: Arc<(Mutex<StateMachine>, Condvar)>) -> JoinHandle<()> {

    println!("===== BEGIN candidate");

    thread::spawn(move || {

        thread::sleep(Duration::from_secs(5));

        let (lock, _cvar) = &*arc_pair;
        let mut state_machine = lock.lock().unwrap();
        state_machine.current_state = ElectionState::Leader;

        println!("===== END candidate");
    })
}

fn leader_behavior(_arc_pair: Arc<(Mutex<StateMachine>, Condvar)>) -> JoinHandle<()> {

    println!("***** BEGIN leader");

    thread::spawn(move || {

        thread::sleep(Duration::from_secs(15));
        println!("***** END leader");
    })
}

fn follower_behavior(arc_pair: Arc<(Mutex<StateMachine>, Condvar)>) -> JoinHandle<()> {

    println!("----- BEGIN follower");

    thread::spawn(move || {

        let (lock, cvar) = &*arc_pair;

        // Loop until an election timeout occurs
        loop {
            println!("-- election: loop start");

            let mut state_machine = lock.lock().unwrap();

            println!("-- election - wait for heartbeat: {}", 0);
            let start = Instant::now();

            let timeout = state_machine.election_timout;
            let result = cvar.wait_timeout(state_machine, Duration::from_millis(timeout)).unwrap();

            let duration = start.elapsed();
            println!("-- election - waited: {}", duration.as_millis());

            state_machine = result.0;

            if state_machine.try_election_timeout() {
                println!("-- election - timeout! Viva la revolucion!");
                break;
            }
            else {
                println!("-- election: received HEARTBEAT -|v-|v-|v-|v-|v-|v-|v");
                continue;
            }
        }

        println!("----- END follower");
    })
}
