# KnociDB Raft Example

This example demonstrates how to build a distributed key-value storage system using KnociDB's Raft consensus module.

## Usage

### Compilation

```bash
go build -o raft-example main.go
```

### Starting the Cluster

#### Start the First Node (as the Initial Node of the Cluster)

```bash
# Windows
.\raft-example --id=1 --addr=localhost:8001 --dir=./node --peers=1=localhost:8001,2=localhost:8002,3=localhost:8003

# Linux/macOS
./raft-example --id=1 --addr=localhost:8001 --dir=./node --peers=1=localhost:8001,2=localhost:8002,3=localhost:8003
```

#### Start the Second Node and Join the Cluster

```bash
# Windows
.\raft-example --id=2 --addr=localhost:8002 --dir=./node --peers=1=localhost:8001,2=localhost:8002,3=localhost:8003 --join

# Linux/macOS
./raft-example --id=2 --addr=localhost:8002 --dir=./node --peers=1=localhost:8001,2=localhost:8002,3=localhost:8003 --join
```

#### Start the Third Node and Join the Cluster

```bash
# Windows
.\raft-example --id=3 --addr=localhost:8003 --dir=./node --peers=1=localhost:8001,2=localhost:8002,3=localhost:8003 --join

# Linux/macOS
./raft-example --id=3 --addr=localhost:8003 --dir=./node --peers=1=localhost:8001,2=localhost:8002,3=localhost:8003 --join
```

### Command Line Arguments

- `--id`: Node ID (must be a unique positive integer)
- `--cluster`: Cluster ID (default is 1)
- `--addr`: Raft node address (format: host:port)
- `--dir`: Data directory
- `--join`: Whether to join an existing cluster as a new node
- `--peers`: List of cluster members (format: id1=host1:port1,id2=host2:port2,...)
- `--sync`: Whether to use synchronous mode (default is true)
- `--interactive`: Whether to enable interactive mode (default is true)

### Interactive Commands

After starting a node, you can interact with the system using the following commands:

- `put <key> <value>`: Write a key-value pair
- `get <key>`: Read a key's value
- `delete <key>`: Delete a key-value pair
- `leader`: Display current Leader information
- `members`: Display cluster members
- `add <id> <addr>`: Add a new node
- `remove <id>`: Remove a node
- `help`: Display help information
- `exit`: Exit the program

## Example Workflow

1. Start three nodes to form a cluster
2. Execute `put name KnociDB` on any node to write data
3. Execute `get name` on other nodes to verify data synchronization
4. Execute `leader` to check the current Leader node
5. Execute `members` to view cluster members

## Notes

- Ensure all nodes have synchronized clocks
- The cluster requires a majority of nodes to be online to function properly (e.g., a three-node cluster needs at least two nodes online)
- In a production environment, different data directories should be configured for each node