use std::thread::{self, JoinHandle};

enum ServerState {
    Follower,
    Candidate,
    Leader
}

pub fn start_consensus() -> JoinHandle<()> {

    let handle = thread::spawn(|| {

        // Initial state
        let state = ServerState::Follower;
    });

    handle
}