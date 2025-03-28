package com.meta.shardmanager.client;

import com.meta.shardmanager.model.Host;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Client library for applications to interact with the shard manager.
 */
public class ShardManagerClient {
    private static final Logger logger = LoggerFactory.getLogger(ShardManagerClient.class);
    
    private final String zkConnectString;
    private final String basePath;
    private final Map<Long, Host> keyHostCache = new ConcurrentHashMap<>();
    private final RoutingTableWatcher routingTableWatcher;
    
    public ShardManagerClient(String zkConnectString, String basePath) {
        this.zkConnectString = zkConnectString;
        this.basePath = basePath;
        this.routingTableWatcher = new RoutingTableWatcher(zkConnectString, basePath, this::updateRoutingTable);
        this.routingTableWatcher.start();
    }
    
    /**
     * Gets the host responsible for a specific key.
     */
    public Host getHostForKey(long key) {
        // Check cache first
        Host cachedHost = keyHostCache.get(key);
        if (cachedHost != null) {
            return cachedHost;
        }
        
        // If not in cache, query the routing table
        Host host = routingTableWatcher.getHostForKey(key);
        if (host != null) {
            keyHostCache.put(key, host);
        }
        
        return host;
    }
    
    /**
     *<boltAction type="file" filePath="src/main/java/com/meta/shardmanager/client/ShardManagerClient.java">
     * Invalidates the routing cache for a specific key.
     */
    public void invalidateCache(long key) {
        keyHostCache.remove(key);
    }
    
    /**
     * Clears the entire routing cache.
     */
    public void clearCache() {
        keyHostCache.clear();
    }
    
    /**
     * Updates the routing table based on changes from ZooKeeper.
     */
    private void updateRoutingTable(Map<Long, Host> newRoutingTable) {
        // Clear the cache when routing table changes
        clearCache();
        logger.info("Routing table updated with {} entries", newRoutingTable.size());
    }
    
    /**
     * Closes the client and releases resources.
     */
    public void close() {
        routingTableWatcher.stop();
    }
}
