# Draft
Draft is replicated in-memory database using the Raft algorithm (https://raft.github.io/).

For the database client see: https://github.com/pouriya-zarbafian/draft-client

# Setup on each node
## Configuration file
Create a configuration file and set the correct value for the property `me`
```
# Comma separated list of members in the form host:port
members=127.0.0.1:8888,127.0.0.1:9999

# This member
me=127.0.0.1:8888

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
cd /path/to/draft_project
export DRAFT_CONFIG=/path/to/config/draft.conf
cargo run
```
