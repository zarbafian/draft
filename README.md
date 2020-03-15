# Draft
Draft is a replicated in-memory key-value database using the Raft algorithm (https://raft.github.io/).

Implementation of the Raft algorithm makes the cluster resilient to server failure.

For the database client see: https://github.com/pouriya-zarbafian/draft-client

# Development status
This version is a first draft (I'll take myself out), and is not ready for production. Large scale testing will follow to assess the correctness and resilience of the system.

# Setup on each node
## Configuration file
Create a configuration file and set the correct value for the property `me`. Currently only IPs are supported.
```
# Comma separated list of members in the form host:port
members=127.0.0.1:7777,127.0.0.1:8888,127.0.0.1:9999

# This member
me=127.0.0.1:7777

# Election timeout and randomness in milliseconds
election.timeout=500
election.randomness=500

# Maximum inactivity period for leader:
# Should be strictly less than election timeout
max.inactivity=400

# Message handling threads
handler.threads=4

# Logging
log.filename=log/node1.log
log.level=INFO
```
## Set environment variable and start server
```
git clone https://github.com/pouriya-zarbafian/draft.git
cd draft
export DRAFT_CONFIG=/path/to/config/draft.conf
cargo run
```
