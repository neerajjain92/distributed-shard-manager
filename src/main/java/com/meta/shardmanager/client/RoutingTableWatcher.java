package com.meta.shardmanager.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.meta.shardmanager.model.Host;
import com.meta.shardmanager.model.Shard;
import com.meta.shardmanager.model.ShardAssignment;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Watches for changes in the shard routing table and updates clients.
 */
public class RoutingTableWatcher {
    private static final Logger logger = LoggerFactory.getLogger(RoutingTableWatcher.class);
    
    private final CuratorFramework client;
    private final String basePath;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Consumer<Map<Long, Host>> routingTableUpdateCallback;
    
    private final Map<String, Shard> shards = new ConcurrentHashMap<>();
    private final Map<String, Host> hosts = new ConcurrentHashMap<>();
    private final Map<String, Map<String, ShardAssignment>> assignments = new ConcurrentHashMap<>();
    private final TreeMap<Long, String> keyToShardMap = new TreeMap<>();
    
    private PathChildrenCache shardsCache;
    private PathChildrenCache hostsCache;
    private PathChildrenCache assignmentsCache;
    
    public RoutingTableWatcher(String zkConnectString, String basePath, 
                              Consumer<Map<Long, Host>> routingTableUpdateCallback) {
        this.basePath = basePath;
        this.routingTableUpdateCallback = routingTableUpdateCallback;
        
        // Initialize ZooKeeper client
        client = CuratorFrameworkFactory.newClient(
                zkConnectString,
                new ExponentialBackoffRetry(1000, 3));
    }
    
    public void start() {
        client.start();
        
        try {
            // Initialize caches
            shardsCache = new PathChildrenCache(client, basePath + "/shards", true);
            hostsCache = new PathChildrenCache(client, basePath + "/hosts", true);
            assignmentsCache = new PathChildrenCache(client, basePath + "/assignments", true);
            
            // Add listeners
            shardsCache.getListenable().addListener((client, event) -> {
                if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED || 
                        event.getType() == PathChildrenCacheEvent.Type.CHILD_UPDATED) {
                    Shard shard = objectMapper.readValue(event.getData().getData(), Shard.class);
                    shards.put(shard.getId(), shard);
                    updateKeyToShardMap();
                } else if (event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
                    String shardId = event.getData().getPath().substring(
                            (basePath + "/shards/").length());
                    shards.remove(shardId);
                    updateKeyToShardMap();
                }
                updateRoutingTable();
            });
            
            hostsCache.getListenable().addListener((client, event) -> {
                if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED || 
                        event.getType() == PathChildrenCacheEvent.Type.CHILD_UPDATED) {
                    Host host = objectMapper.readValue(event.getData().getData(), Host.class);
                    hosts.put(host.getId(), host);
                } else if (event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
                    String hostId = event.getData().getPath().substring(
                            (basePath + "/hosts/").length());
                    hosts.remove(hostId);
                }
                updateRoutingTable();
            });
            
            assignmentsCache.getListenable().addListener((client, event) -> {
                if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED || 
                        event.getType() == PathChildrenCacheEvent.Type.CHILD_UPDATED) {
                    ShardAssignment assignment = objectMapper.readValue(
                            event.getData().getData(), ShardAssignment.class);
                    assignments.computeIfAbsent(assignment.getShardId(), k -> new ConcurrentHashMap<>())
                            .put(assignment.getHostId(), assignment);
                } else if (event.getType() == PathChildrenCacheEvent.Type.CHILD_REMOVED) {
                    String path = event.getData().getPath().substring(
                            (basePath + "/assignments/").length());
                    String[] parts = path.split("-");
                    if (parts.length == 2) {
                        String shardId = parts[0];
                        String hostId = parts[1];
                        Map<String, ShardAssignment> shardAssignments = assignments.get(shardId);
                        if (shardAssignments != null) {
                            shardAssignments.remove(hostId);
                            if (shardAssignments.isEmpty()) {
                                assignments.remove(shardId);
                            }
                        }
                    }
                }
                updateRoutingTable();
            });
            
            // Start caches
            shardsCache.start();
            hostsCache.start();
            assignmentsCache.start();
            
        } catch (Exception e) {
            logger.error("Failed to start routing table watcher", e);
            throw new RuntimeException("Failed to start routing table watcher", e);
        }
    }
    
    private void updateKeyToShardMap() {
        keyToShardMap.clear();
        for (Shard shard : shards.values()) {
            keyToShardMap.put(shard.getRange().getStartKey(), shard.getId());
        }
    }
    
    private void updateRoutingTable() {
        Map<Long, Host> routingTable = new HashMap<>();
        
        for (Map.Entry<String, Shard> entry : shards.entrySet()) {
            String shardId = entry.getKey();
            Shard shard = entry.getValue();
            
            // Find the primary host for this shard
            Map<String, ShardAssignment> shardAssignments = assignments.get(shardId);
            if (shardAssignments != null) {
                for (ShardAssignment assignment : shardAssignments.values()) {
                    if (assignment.getRole() == ShardAssignment.AssignmentRole.PRIMARY && 
                            assignment.getState() == ShardAssignment.AssignmentState.ACTIVE) {
                        Host host = hosts.get(assignment.getHostId());
                        if (host != null) {
                            // Add entry for each key in the shard's range
                            for (long key = shard.getRange().getStartKey(); 
                                 key < shard.getRange().getEndKey(); 
                                 key++) {
                                routingTable.put(key, host);
                            }
                            break;
                        }
                    }
                }
            }
        }
        
        // Notify callback
        routingTableUpdateCallback.accept(routingTable);
    }
    
    public Host getHostForKey(long key) {
        // Find the shard for this key
        Map.Entry<Long, String> entry = keyToShardMap.floorEntry(key);
        if (entry == null) {
            return null;
        }
        
        String shardId = entry.getValue();
        Shard shard = shards.get(shardId);
        
        if (shard == null || !shard.getRange().contains(key)) {
            return null;
        }
        
        // Find the primary host for this shard
        Map<String, ShardAssignment> shardAssignments = assignments.get(shardId);
        if (shardAssignments != null) {
            for (ShardAssignment assignment : shardAssignments.values()) {
                if (assignment.getRole() == ShardAssignment.AssignmentRole.PRIMARY && 
                        assignment.getState() == ShardAssignment.AssignmentState.ACTIVE) {
                    return hosts.get(assignment.getHostId());
                }
            }
        }
        
        return null;
    }
    
    public void stop() {
        try {
            if (shardsCache != null) {
                shardsCache.close();
            }
            if (hostsCache != null) {
                hostsCache.close();
            }
            if (assignmentsCache != null) {
                assignmentsCache.close();
            }
        } catch (Exception e) {
            logger.error("Error closing caches", e);
        }
        
        client.close();
    }
}
