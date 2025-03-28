package com.meta.shardmanager;

import com.meta.shardmanager.core.ShardManager;
import com.meta.shardmanager.core.ShardPlacementPolicy;
import com.meta.shardmanager.model.Host;
import com.meta.shardmanager.model.ShardRange;
import com.meta.shardmanager.store.MetadataStore;
import com.meta.shardmanager.store.ZookeeperMetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main server class for the Shard Manager.
 */
public class ShardManagerServer {
    private static final Logger logger = LoggerFactory.getLogger(ShardManagerServer.class);
    
    private final ShardManager shardManager;
    private final MetadataStore metadataStore;
    
    public ShardManagerServer(String zkConnectString, String basePath, int replicationFactor) {
        logger.info("Starting Shard Manager Server with ZooKeeper at {}, base path {}, replication factor {}",
                zkConnectString, basePath, replicationFactor);
        
        // Initialize metadata store
        this.metadataStore = new ZookeeperMetadataStore(zkConnectString, basePath);
        
        // Initialize shard placement policy
        ShardPlacementPolicy placementPolicy = new ShardPlacementPolicy();
        
        // Initialize shard manager
        this.shardManager = new ShardManager(metadataStore, placementPolicy, replicationFactor);
        
        logger.info("Shard Manager Server started successfully");
    }
    
    public void createInitialShards(int numShards, long keyRangeStart, long keyRangeEnd) {
        logger.info("Creating {} initial shards for key range {} to {}", 
                numShards, keyRangeStart, keyRangeEnd);
        
        long rangeSize = (keyRangeEnd - keyRangeStart) / numShards;
        
        for (int i = 0; i < numShards; i++) {
            long start = keyRangeStart + (i * rangeSize);
            long end = (i == numShards - 1) ? keyRangeEnd : start + rangeSize;
            
            ShardRange range = new ShardRange(start, end);
            shardManager.createShard(range);
        }
    }
    
    public void registerHost(String hostname, int port, int capacity) {
        String hostId = hostname + ":" + port;
        Host host = new Host(hostId, hostname, port, capacity);
        shardManager.registerHost(host);
    }
    
    public void shutdown() {
        logger.info("Shutting down Shard Manager Server");
        shardManager.shutdown();
        if (metadataStore instanceof ZookeeperMetadataStore) {
            ((ZookeeperMetadataStore) metadataStore).close();
        }
    }
    
    public static void main(String[] args) {
        // Default configuration
        String zkConnectString = "localhost:2181";
        String basePath = "/shard-manager";
        int replicationFactor = 3;
        
        // Parse command line arguments
        for (int i = 0; i < args.length; i++) {
            if ("-zk".equals(args[i]) && i + 1 < args.length) {
                zkConnectString = args[++i];
            } else if ("-path".equals(args[i]) && i + 1 < args.length) {
                basePath = args[++i];
            } else if ("-rf".equals(args[i]) && i + 1 < args.length) {
                replicationFactor = Integer.parseInt(args[++i]);
            }
        }
        
        // Create and start the server
        ShardManagerServer server = new ShardManagerServer(zkConnectString, basePath, replicationFactor);
        
        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown));
        
        // For demo purposes, create some initial shards and hosts
        server.createInitialShards(10, 0, 1000000);
        server.registerHost("host1.example.com", 8080, 100);
        server.registerHost("host2.example.com", 8080, 100);
        server.registerHost("host3.example.com", 8080, 100);
        server.registerHost("host4.example.com", 8080, 100);
        server.registerHost("host5.example.com", 8080, 100);
        
        logger.info("Shard Manager Server is running. Press Ctrl+C to stop.");
        
        // Keep the main thread alive
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
