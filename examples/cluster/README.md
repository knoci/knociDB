# KnociDB Cluster Example

This example demonstrates KnociDB's L1 transparent cluster: a single Raft group with leader-aware write forwarding. With the internal RPC addresses configured, `put/get/delete` can be sent to any node.

## Usage

### Compilation

```bash
go build -o cluster-example main.go
```

### Starting the Cluster

#### Start the First Node (Bootstrap)

```bash
./cluster-example --id=1 --addr=localhost:8001 --rpc=localhost:9001 \
  --dir=./node --peers=1=localhost:8001,2=localhost:8002,3=localhost:8003 \
  --rpc-peers=1=localhost:9001,2=localhost:9002,3=localhost:9003
```

#### Start the Second and Third Nodes

```bash
./cluster-example --id=2 --addr=localhost:8002 --rpc=localhost:9002 \
  --dir=./node --peers=1=localhost:8001,2=localhost:8002,3=localhost:8003 \
  --rpc-peers=1=localhost:9001,2=localhost:9002,3=localhost:9003 --join

./cluster-example --id=3 --addr=localhost:8003 --rpc=localhost:9003 \
  --dir=./node --peers=1=localhost:8001,2=localhost:8002,3=localhost:8003 \
  --rpc-peers=1=localhost:9001,2=localhost:9002,3=localhost:9003 --join
```

On Windows, use `.\cluster-example` instead of `./cluster-example`.

### Command Line Arguments

- `--id`: Node ID (must be a unique positive integer)
- `--cluster`: Cluster ID (default is 1)
- `--addr`: Dragonboat Raft address (format: host:port)
- `--rpc`: Internal forwarding RPC address (optional)
- `--dir`: Data directory
- `--join`: Whether to join an existing cluster as a new node
- `--peers`: List of cluster members (format: id1=host1:port1,id2=host2:port2,...)
- `--rpc-peers`: NodeID -> forwarding RPC address (required for follower forwarding)
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
2. Execute `put name KnociDB` on a follower; the node forwards to the leader
3. Execute `get name` on other nodes to verify data synchronization
4. Execute `leader` to check the current Leader node
5. Execute `members` to view cluster members

## Notes

- Ensure all nodes have synchronized clocks
- The cluster requires a majority of nodes to be online to function properly (e.g., a three-node cluster needs at least two nodes online)
- In a production environment, different data directories should be configured for each node