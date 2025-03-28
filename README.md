# Shard Manager Implementation

This is a Java implementation of a Shard Manager system inspired by Meta's Shard Manager. It provides a framework for managing distributed data across multiple hosts with automatic sharding, replication, failover, and load balancing.

## Key Components

### Core Components

1. **ShardManager**: The central component that manages shard allocation, rebalancing, and failover.
2. **FailoverManager**: Handles host failures and recovery, including promoting secondaries to primaries.
3. **ShardPlacementPolicy**: Determines the optimal placement of shards across hosts.

### Data Model

1. **Shard**: Represents a unit of data partitioning with a key range.
2. **Host**: Represents a server that can store and serve shards.
3. **ShardAssignment**: Maps shards to hosts with specific roles (PRIMARY, SECONDARY).

### Storage

1. **MetadataStore**: Interface for storing and retrieving metadata.
2. **ZookeeperMetadataStore**: Implementation using ZooKeeper for distributed coordination.

### Client

1. **ShardManagerClient**: Client library for applications to interact with the shard manager.
2. **RoutingTableWatcher**: Watches for changes in the shard routing table and updates clients.

## Key Features

### Sharding

The system divides data into shards based on key ranges. Each shard is responsible for a specific range of keys, allowing horizontal scaling of data across multiple hosts.

### Replication

Each shard is replicated across multiple hosts for fault tolerance. The replication factor is configurable, with one host designated as the PRIMARY and others as SECONDARY.

### Failover

The system automatically detects host failures and initiates failover:
1. When a PRIMARY host fails, a SECONDARY is promoted to PRIMARY.
2. New replicas are created to maintain the desired replication factor.
3. Clients are automatically updated with the new routing information.

### Failback

When a failed host recovers:
1. The host is marked as ONLINE.
2. Shards are gradually rebalanced to include the recovered host.
3. The host may become PRIMARY for some shards during rebalancing.

### Load Balancing

The system periodically rebalances shards across hosts to ensure even distribution:
1. Overloaded hosts have shards moved to underloaded hosts.
2. The rebalancing process is gradual to minimize impact on performance.
3. The placement policy considers host load, capacity, and fault domains.

### Routing

Clients use the ShardManagerClient to determine which host to contact for a specific key:
1. The client maintains a local cache of the routing table.
2. The routing table is automatically updated when shards move or hosts change.
3. The client handles retries and redirects when routing information changes.

## Implementation Details

### Metadata Management

ZooKeeper is used for storing and coordinating metadata:
1. Shard definitions and assignments are stored in ZooKeeper.
2. Watches are set up to detect changes in real-time.
3. The distributed nature of ZooKeeper ensures consistency across all components.

### Shard Operations

The system supports various shard operations:
1. **Creation**: New shards can be created with specific key ranges.
2. **Splitting**: A shard can be split into multiple shards to distribute load.
3. **Merging**: Multiple shards can be merged into a single shard to reduce overhead.
4. **Moving**: A shard can be moved from one host to another for load balancing.

### Host Management

Hosts can be dynamically added and removed:
1. **Registration**: New hosts can be registered with the system.
2. **Deregistration**: Hosts can be gracefully removed by draining shards.
3. **Health Monitoring**: Host health is continuously monitored for quick failure detection.

## Usage Example

See the `ShardedDataService` class for an example of how to use the Shard Manager in an application.

## Running the System

1. Start ZooKeeper:
   ```
   zookeeper-server-start.sh config/zookeeper.properties
   ```

2. Start the Shard Manager Server:
   ```
   java -jar shard-manager.jar -zk localhost:2181 -path /shard-manager -rf 3
   ```

3. Run your application with the Shard Manager Client:
   ```java
   ShardManagerClient client = new ShardManagerClient("localhost:2181", "/shard-manager");
   Host host = client.getHostForKey(123);
   // Connect to the host and perform operations
   ```
