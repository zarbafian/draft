use std::sync::{Arc, Mutex, Condvar};
use rand::Rng;
use std::time::{Instant, Duration};
use log::{trace, info, warn};
use std::thread;

use crate::server::Server;
use crate::net;
use crate::message::VoteRequest;

type Behavior = Box<dyn FnOnce()->Type + Sync + 'static>;

enum Type {
    Follower,
    Candidate,
    Leader,
}

/// Start the consensus management behavior
pub fn start_consensus_behavior(timeout_pair: Arc<(Mutex<Server>, Condvar)>) {
    thread::Builder::new()
        .name("behavior".into())
        .spawn(move ||{
            // Start as a follower
            let mut next_behavior = Type::Follower;

            // Server behavior loop
            loop {
                let behavior = match next_behavior {
                    Type::Follower => follower_behavior(timeout_pair.clone()),
                    Type::Candidate => candidate_behavior(timeout_pair.clone()),
                    Type::Leader => leader_behavior(timeout_pair.clone())
                };

                // Upon completion, a behavior returns the next behavior type
                next_behavior = behavior();
            }
        })
        .unwrap();
}

/// Returns a closure that act as a follower.
///
/// # Returns
///
/// The next behavior for the server.
fn follower_behavior(timeout_pair: Arc<(Mutex<Server>, Condvar)>) -> Behavior {
    Box::new(move || {
        info!("follower started");
        {
            let timeout_loop_pair = timeout_pair.clone();

            let (lock, _timeout_cvar) = &*timeout_loop_pair;
            let mut server = lock.lock().unwrap();

            server.init_follower();
        }

        let mut rand = rand::thread_rng();

        // Outer loop for handling heartbeats
        'outer: loop {
            trace!("follower: timeout loop start");

            let timeout_loop_pair = timeout_pair.clone();

            let (lock, timeout_cvar) = &*timeout_loop_pair;
            let mut server = lock.lock().unwrap();

            // Randomize election timeout in the range [election_timeout, election_timeout + election_randomness[
            let timeout = server.get_election_timeout() + rand.gen_range(0, server.get_election_randomness());

            trace!("follower: timeout is {} ms for heartbeat", timeout);

            let start = Instant::now();
            let mut elapsed = 0;

            // Inner loop to make sure we waited the full length of the timeout
            'inner: loop {
                let result = timeout_cvar.wait_timeout(server, Duration::from_millis(timeout - elapsed)).unwrap();

                elapsed = start.elapsed().as_millis() as u64;
                server = result.0;

                if server.reset_election_timeout {
                    // Heartbeat received: reset flag
                    server.reset_election_timeout = false;
                    break; // inner loop
                }
                if result.1.timed_out() || elapsed >= timeout {
                    // Timeout reached
                    trace!("follower ended: election timeout reached");
                    return Type::Candidate;
                }
                else {
                    // Neither timeout nor heartbeat: keep waiting
                    continue;
                }
            }
        }
    })
}

/// Returns a closure that act as a candidate.
///
/// # Returns
///
/// The next behavior for the server.
fn candidate_behavior(timeout_pair: Arc<(Mutex<Server>, Condvar)>) -> Behavior {
    Box::new(move || {
        info!("candidate started");

        let mut rand = rand::thread_rng();
        let timeout;
        {
            let (lock, _timeout_cvar) = &*timeout_pair;
            let mut server = lock.lock().unwrap();

            // Setup for a new election
            server.init_election();

            // Election timeout
            timeout = server.get_election_timeout() + rand.gen_range(0, server.get_election_randomness());

            // Latest log entry
            let (last_term, last_index) = server.last_entry_indices();

            // Send vote requests to the other members
            net::broadcast_vote_request(VoteRequest{
                term: server.get_current_term(),
                candidate_id: server.id(),
                last_log_index: last_index,
                last_log_term: last_term,
            }, server.get_other_servers());
        }

        let start = Instant::now();
        let mut elapsed = 0;

        trace!("candidate - election timeout is {} ms", timeout);

        loop {
            let (lock, timeout_cvar) = &*timeout_pair;
            let mut server = lock.lock().unwrap();

            let result = timeout_cvar.wait_timeout(server, Duration::from_millis(timeout - elapsed)).unwrap();

            elapsed = start.elapsed().as_millis() as u64;
            server = result.0;

            if server.was_elected() {
                // Was elected leader
                info!("candidate ended: election won");
                return Type::Leader;
            }
            else if server.reset_election_timeout {
                // Heartbeat received: another leader was elected
                info!("candidate ended: received message from leader, will step down");
                return Type::Follower;
            }
            else if server.convert_to_follower {
                // Heartbeat received: another leader was elected
                info!("candidate ended: received message from leader, will step down");
                return Type::Follower;
            }
            else if result.1.timed_out() || elapsed >= timeout {
                // Timeout reached
                info!("candidate ended: election timeout reached: timed_out()={}, elapsed={}", result.1.timed_out(), elapsed);
                return Type::Candidate;
            }
            else {
                // Neither timeout nor winning or losing election: keep waiting
                continue;
            }
        }
    })
}

/// Returns a closure that act as a leader,
///
/// # Returns
///
/// The next behavior for the server.
fn leader_behavior(timeout_pair: Arc<(Mutex<Server>, Condvar)>) -> Behavior {
    Box::new(move || {
        info!("leader started");
        {
            let (lock, _timeout_cvar) = &*timeout_pair.clone();
            let mut server = lock.lock().unwrap();

            // Setup as leader
            server.init_leader();

            // Send initial heartbeat
            let initial_append = server.get_initial_entries();

            net::broadcast_append_entries_request(initial_append);
        }

        loop {
            let (lock, timeout_cvar) = &*timeout_pair.clone();
            let mut server = lock.lock().unwrap();

            // Set maximum inactivity from leader to a percentage of the election timeout
            let max_inactivity = server.get_max_inactivity();

            let result = timeout_cvar.wait_timeout(server, Duration::from_millis(max_inactivity)).unwrap();

            server = result.0;

            // Received a higher term, convert to follower
            if server.convert_to_follower {
                warn!("leader ended");
                return Type::Follower;
            }

            // Client request received, woken up for replication
            if server.client_request_received {
                server.client_request_received = false;
            }
//

            // The new entries for each member
            let entries_map = server.get_next_entries();

            net::broadcast_append_entries_request(entries_map);

            // TODO: remove this
            server.print_logs();
        }
    })
}
