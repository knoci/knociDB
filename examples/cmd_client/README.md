# KnociDB Command Client

This is an interactive command line client that allows users to directly connect to and operate the KnociDB database.

## Features

- Supports basic CRUD operations (GET, PUT, DEL, etc.)
- Provides an interactive command line interface
- Supports standalone mode and distributed mode

## Usage

### Compilation

```bash
cd examples/cmd_client
go build -o knocidb-cli
```

### Running

#### Standalone Mode

```bash
# Using default configuration
./knocidb-cli

# Specify data directory
./knocidb-cli --dir /path/to/data

# Specify number of partitions
./knocidb-cli --partitions 3
```

#### Distributed Mode

```bash
# Start node 1 (as the first node of the cluster)
./knocidb-cli --distributed --dir ./node --id 1 --listen 127.0.0.1:8001 --peers 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003

# Start node 2 and join the cluster
./knocidb-cli --distributed --dir ./node --id 2 --listen 127.0.0.1:8002  --join

# Start node 3 and join the cluster
./knocidb-cli --distributed --dir ./node --id 3 --listen 127.0.0.1:8003  --join
```

#### Distributed Mode Specific Commands

```
# View cluster information
CLUSTER INFO

# Add a new node to the cluster
CLUSTER ADD <node_id> <address>

# Remove a node from the cluster
CLUSTER REMOVE <node_id>

# View detailed information (including cluster status)
CLUSTER STATUS
```

## Supported Commands

- `GET key` - Get the value of the specified key
- `PUT key value` - Set the value of a key
- `DEL key [key ...]` - Delete one or more keys
- `EXISTS key [key ...]` - Check if one or more keys exist
- `PING [message]` - Test connection, returns PONG or the specified message
- `BEGIN` - Start batch processing
- `COMMIT` - Commit batch processing
- `ECHO message` - Return the same message
- `INFO` - Display server information
- `HELP` - Display help information
- `EXIT` - Exit the client

## Examples

```
KnociDB
use 'HELP' to get help，'EXIT' to exit the client
knocidb> SET mykey hello
OK
knocidb> GET mykey
"hello"
knocidb> EXISTS mykey
(integer) 1
knocidb> DEL mykey
(integer) 1
knocidb> GET mykey
(nil)
knocidb> PING
PONG
knocidb> INFO
# knocidb
mode standalone

# Data
db_path ./data
partitions 3
knocidb> EXIT
see you next time~
```