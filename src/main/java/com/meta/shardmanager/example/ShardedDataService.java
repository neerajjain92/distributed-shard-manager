package com.meta.shardmanager.example;

import com.meta.shardmanager.client.ShardManagerClient;
import com.meta.shardmanager.model.Host;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Example service that uses the Shard Manager to store and retrieve data.
 */
public class ShardedDataService {
    private static final Logger logger = LoggerFactory.getLogger(ShardedDataService.class);
    
    private final ShardManagerClient shardManagerClient;
    private final Map<Host, DataStoreClient> clientCache = new ConcurrentHashMap<>();
    
    public ShardedDataService(String zkConnectString, String basePath) {
        this.shardManagerClient = new ShardManagerClient(zkConnectString, basePath);
    }
    
    /**
     * Stores a value for a key.
     */
    public void put(long key, String value) {
        Host host = shardManagerClient.getHostForKey(key);
        if (host == null) {
            throw new RuntimeException("No host found for key: " + key);
        }
        
        DataStoreClient client = getOrCreateClient(host);
        client.put(key, value);
    }
    
    /**
     * Retrieves a value for a key.
     */
    public String get(long key) {
        Host host = shardManagerClient.getHostForKey(key);
        if (host == null) {
            throw new RuntimeException("No host found for key: " + key);
        }
        
        DataStoreClient client = getOrCreateClient(host);
        return client.get(key);
    }
    
    /**
     * Deletes a key.
     */
    public void delete(long key) {
        Host host = shardManagerClient.getHostForKey(key);
        if (host == null) {
            throw new RuntimeException("No host found for key: " + key);
        }
        
        DataStoreClient client = getOrCreateClient(host);
        client.delete(key);
    }
    
    private DataStoreClient getOrCreateClient(Host host) {
        return clientCache.computeIfAbsent(host, h -> new DataStoreClient(h.getHostname(), h.getPort()));
    }
    
    /**
     * Closes the service and releases resources.
     */
    public void close() {
        shardManagerClient.close();
        for (DataStoreClient client : clientCache.values()) {
            client.close();
        }
    }
    
    /**
     * Example client for a data store service.
     */
    private static class DataStoreClient {
        private final String hostname;
        private final int port;
        private final Map<Long, String> localCache = new HashMap<>();
        
        public DataStoreClient(String hostname, int port) {
            this.hostname = hostname;
            this.port = port;
            logger.info("Created client for {}:{}", hostname, port);
        }
        
        public void put(long key, String value) {
            // In a real implementation, this would make a network call to the actual data store
            logger.info("PUT key={} value={} to {}:{}", key, value, hostname, port);
            localCache.put(key, value);
        }
        
        public String get(long key) {
            // In a real implementation, this would make a network call to the actual data store
            String value = localCache.get(key);
            logger.info("GET key={} value={} from {}:{}", key, value, hostname, port);
            return value;
        }
        
        public void delete(long key) {
            // In a real implementation, this would make a network call to the actual data store
            logger.info("DELETE key={} from {}:{}", key, hostname, port);
            localCache.remove(key);
        }
        
        public void close() {
            // Close connections, etc.
            logger.info("Closed client for {}:{}", hostname, port);
        }
    }
    
    public static void main(String[] args) {
        // Example usage
        ShardedDataService service = new ShardedDataService("localhost:2181", "/shard-manager");
        
        try {
            // Store some data
            service.put(1, "value1");
            service.put(500000, "value2");
            service.put(999999, "value3");
            
            // Retrieve data
            System.out.println("Key 1: " + service.get(1));
            System.out.println("Key 500000: " + service.get(500000));
            System.out.println("Key 999999: " + service.get(999999));
            
            // Delete data
            service.delete(1);
            
            // Try to get deleted data
            System.out.println("Key 1 after delete: " + service.get(1));
            
        } finally {
            service.close();
        }
    }
}
