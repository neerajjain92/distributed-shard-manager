package com.meta.shardmanager.core;

import com.meta.shardmanager.model.*;
import com.meta.shardmanager.store.MetadataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * The main Shard Manager class that handles shard allocation, rebalancing, and failover.
 */
public class ShardManager {
    private static final Logger logger = LoggerFactory.getLogger(ShardManager.class);
    
    private final MetadataStore metadataStore;
    private final Map<String, Shard> shards = new ConcurrentHashMap<>();
    private final Map<String, Host> hosts = new ConcurrentHashMap<>();
    private final Map<String, Map<String, ShardAssignment>> assignments = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final ShardPlacementPolicy placementPolicy;
    private final FailoverManager failoverManager;
    private final int replicationFactor;
    
    public ShardManager(MetadataStore metadataStore, ShardPlacementPolicy placementPolicy, int replicationFactor) {
        this.metadataStore = metadataStore;
        this.placementPolicy = placementPolicy;
        this.replicationFactor = replicationFactor;
        this.failoverManager = new FailoverManager(this);
        
        // Initialize the shard manager
        initialize();
        
        // Schedule periodic tasks
        scheduler.scheduleAtFixedRate(this::monitorHosts, 10, 10, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(this::rebalanceShards, 60, 60, TimeUnit.SECONDS);
    }
    
    private void initialize() {
        logger.info("Initializing Shard Manager");
        
        // Load hosts from metadata store
        metadataStore.getHosts().forEach(host -> hosts.put(host.getId(), host));
        logger.info("Loaded {} hosts from metadata store", hosts.size());
        
        // Load shards from metadata store
        metadataStore.getShards().forEach(shard -> shards.put(shard.getId(), shard));
        logger.info("Loaded {} shards from metadata store", shards.size());
        
        // Load assignments from metadata store
        metadataStore.getAssignments().forEach(assignment -> {
            assignments.computeIfAbsent(assignment.getShardId(), k -> new ConcurrentHashMap<>())
                    .put(assignment.getHostId(), assignment);
        });
        logger.info("Loaded assignments for {} shards from metadata store", assignments.size());
        
        // Verify and repair any inconsistencies
        verifyAndRepairAssignments();
    }
    
    private void verifyAndRepairAssignments() {
        logger.info("Verifying and repairing shard assignments");
        
        // Check for shards without enough replicas
        for (Shard shard : shards.values()) {
            Map<String, ShardAssignment> shardAssignments = assignments.getOrDefault(shard.getId(), Collections.emptyMap());
            
            // If shard has no assignments or fewer than replication factor, assign it
            if (shardAssignments.size() < replicationFactor) {
                logger.warn("Shard {} has {} replicas, needs {}", shard.getId(), 
                        shardAssignments.size(), replicationFactor);
                assignShard(shard);
            }
        }
        
        // Check for hosts with invalid assignments
        for (Host host : hosts.values()) {
            if (host.getState() == HostState.OFFLINE) {
                // Reassign shards from offline hosts
                reassignShardsFromHost(host.getId());
            }
        }
    }
    
    public void createShard(ShardRange range) {
        Shard shard = new Shard(range);
        shards.put(shard.getId(), shard);
        metadataStore.saveShard(shard);
        assignShard(shard);
        logger.info("Created new shard: {}", shard);
    }
    
    public void registerHost(Host host) {
        hosts.put(host.getId(), host);
        metadataStore.saveHost(host);
        logger.info("Registered new host: {}", host);
        
        // Rebalance shards to include the new host
        rebalanceShards();
    }
    
    public void deregisterHost(String hostId) {
        Host host = hosts.get(hostId);
        if (host != null) {
            host.setState(HostState.DRAINING);
            metadataStore.saveHost(host);
            logger.info("Host {} is now draining", hostId);
            
            // Reassign shards from this host
            reassignShardsFromHost(hostId);
        }
    }
    
    private void assignShard(Shard shard) {
        logger.info("Assigning shard: {}", shard.getId());
        
        // Get suitable hosts for this shard based on placement policy
        List<Host> suitableHosts = placementPolicy.selectHostsForShard(shard, 
                new ArrayList<>(hosts.values()), replicationFactor);
        
        if (suitableHosts.size() < replicationFactor) {
            logger.warn("Not enough suitable hosts for shard {}. Found {} hosts, need {}", 
                    shard.getId(), suitableHosts.size(), replicationFactor);
        }
        
        // Assign primary role to the first host
        if (!suitableHosts.isEmpty()) {
            Host primaryHost = suitableHosts.get(0);
            assignShardToHost(shard.getId(), primaryHost.getId(), ShardAssignment.AssignmentRole.PRIMARY);
            suitableHosts.remove(0);
        }
        
        // Assign secondary roles to the remaining hosts
        for (Host host : suitableHosts) {
            assignShardToHost(shard.getId(), host.getId(), ShardAssignment.AssignmentRole.SECONDARY);
        }
    }
    
    private void assignShardToHost(String shardId, String hostId, ShardAssignment.AssignmentRole role) {
        Host host = hosts.get(hostId);
        Shard shard = shards.get(shardId);
        
        if (host == null || shard == null) {
            logger.error("Cannot assign shard {} to host {}: host or shard not found", shardId, hostId);
            return;
        }
        
        // Create and store the assignment
        ShardAssignment assignment = new ShardAssignment(shardId, hostId, role);
        assignments.computeIfAbsent(shardId, k -> new ConcurrentHashMap<>()).put(hostId, assignment);
        metadataStore.saveAssignment(assignment);
        
        // Update host's assigned shards
        host.assignShard(shardId);
        metadataStore.saveHost(host);
        
        // Update shard state
        shard.setState(ShardState.ASSIGNED);
        metadataStore.saveShard(shard);
        
        logger.info("Assigned shard {} to host {} with role {}", shardId, hostId, role);
    }
    
    private void reassignShardsFromHost(String hostId) {
        logger.info("Reassigning shards from host: {}", hostId);
        
        // Find all shards assigned to this host
        List<String> shardsToReassign = new ArrayList<>();
        for (Map.Entry<String, Map<String, ShardAssignment>> entry : assignments.entrySet()) {
            if (entry.getValue().containsKey(hostId)) {
                shardsToReassign.add(entry.getKey());
            }
        }
        
        // Reassign each shard
        for (String shardId : shardsToReassign) {
            Shard shard = shards.get(shardId);
            if (shard != null) {
                // Remove the assignment from this host
                Map<String, ShardAssignment> shardAssignments = assignments.get(shardId);
                ShardAssignment oldAssignment = shardAssignments.remove(hostId);
                metadataStore.deleteAssignment(oldAssignment);
                
                // If this was the primary, promote a secondary
                if (oldAssignment.getRole() == ShardAssignment.AssignmentRole.PRIMARY) {
                    promoteSecondaryToPrimary(shardId);
                }
                
                // Find a new host for this shard
                List<Host> currentHosts = shardAssignments.keySet().stream()
                        .map(hosts::get)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
                
                Host newHost = placementPolicy.selectNewHostForShard(shard, 
                        new ArrayList<>(hosts.values()), currentHosts);
                
                if (newHost != null) {
                    assignShardToHost(shardId, newHost.getId(), ShardAssignment.AssignmentRole.SECONDARY);
                } else {
                    logger.warn("Could not find a new host for shard {}", shardId);
                }
            }
        }
    }
    
    private void promoteSecondaryToPrimary(String shardId) {
        logger.info("Promoting secondary to primary for shard: {}", shardId);
        
        Map<String, ShardAssignment> shardAssignments = assignments.get(shardId);
        if (shardAssignments == null || shardAssignments.isEmpty()) {
            logger.warn("No assignments found for shard {}", shardId);
            return;
        }
        
        // Find a secondary to promote
        Optional<ShardAssignment> secondaryOpt = shardAssignments.values().stream()
                .filter(a -> a.getRole() == ShardAssignment.AssignmentRole.SECONDARY && 
                        a.getState() == ShardAssignment.AssignmentState.ACTIVE)
                .findFirst();
        
        if (secondaryOpt.isPresent()) {
            ShardAssignment secondary = secondaryOpt.get();
            
            // Create a new primary assignment
            ShardAssignment newPrimary = new ShardAssignment(
                    secondary.getShardId(),
                    secondary.getHostId(),
                    ShardAssignment.AssignmentRole.PRIMARY,
                    secondary.getState()
            );
            
            // Update in memory and metadata store
            shardAssignments.put(secondary.getHostId(), newPrimary);
            metadataStore.deleteAssignment(secondary);
            metadataStore.saveAssignment(newPrimary);
            
            logger.info("Promoted host {} to primary for shard {}", 
                    newPrimary.getHostId(), newPrimary.getShardId());
        } else {
            logger.warn("No suitable secondary found to promote for shard {}", shardId);
        }
    }
    
    private void monitorHosts() {
        logger.debug("Monitoring host health");
        
        for (Host host : hosts.values()) {
            boolean isHealthy = failoverManager.checkHostHealth(host);
            
            if (!isHealthy && host.getState() == HostState.ONLINE) {
                logger.warn("Host {} is unhealthy, marking as OFFLINE", host.getId());
                host.setState(HostState.OFFLINE);
                metadataStore.saveHost(host);
                
                // Trigger failover for this host
                failoverManager.handleHostFailure(host.getId());
            } else if (isHealthy && host.getState() == HostState.OFFLINE) {
                logger.info("Host {} is healthy again, marking as ONLINE", host.getId());
                host.setState(HostState.ONLINE);
                metadataStore.saveHost(host);
                
                // Trigger failback
                failoverManager.handleHostRecovery(host.getId());
            }
        }
    }
    
    private void rebalanceShards() {
        logger.info("Rebalancing shards across hosts");
        
        // Get online hosts
        List<Host> onlineHosts = hosts.values().stream()
                .filter(h -> h.getState() == HostState.ONLINE)
                .collect(Collectors.toList());
        
        if (onlineHosts.size() < 2) {
            logger.info("Not enough online hosts for rebalancing");
            return;
        }
        
        // Calculate average load
        double totalShards = assignments.values().stream()
                .mapToLong(Map::size)
                .sum();
        double avgShardsPerHost = totalShards / onlineHosts.size();
        
        logger.info("Average shards per host: {}", avgShardsPerHost);
        
        // Find overloaded and underloaded hosts
        List<Host> overloadedHosts = onlineHosts.stream()
                .filter(h -> h.getAssignedShardIds().size() > avgShardsPerHost * 1.1)
                .sorted(Comparator.comparingInt(h -> -h.getAssignedShardIds().size()))
                .collect(Collectors.toList());
        
        List<Host> underloadedHosts = onlineHosts.stream()
                .filter(h -> h.getAssignedShardIds().size() < avgShardsPerHost * 0.9 && h.hasCapacity())
                .sorted(Comparator.comparingInt(h -> h.getAssignedShardIds().size()))
                .collect(Collectors.toList());
        
        logger.info("Found {} overloaded hosts and {} underloaded hosts", 
                overloadedHosts.size(), underloadedHosts.size());
        
        // Move shards from overloaded to underloaded hosts
        for (Host overloaded : overloadedHosts) {
            if (underloadedHosts.isEmpty()) break;
            
            int shardsToMove = (int) (overloaded.getAssignedShardIds().size() - avgShardsPerHost);
            if (shardsToMove <= 0) continue;
            
            logger.info("Moving {} shards from host {}", shardsToMove, overloaded.getId());
            
            // Find shards to move
            List<String> candidateShards = new ArrayList<>(overloaded.getAssignedShardIds());
            Collections.shuffle(candidateShards); // Randomize to avoid moving the same shards
            
            for (int i = 0; i < Math.min(shardsToMove, candidateShards.size()); i++) {
                if (underloadedHosts.isEmpty()) break;
                
                String shardId = candidateShards.get(i);
                Host underloaded = underloadedHosts.get(0);
                
                // Move the shard
                moveShard(shardId, overloaded.getId(), underloaded.getId());
                
                // Update underloaded hosts list
                if (!underloaded.hasCapacity() || 
                        underloaded.getAssignedShardIds().size() >= avgShardsPerHost) {
                    underloadedHosts.remove(0);
                }
            }
        }
    }
    
    private void moveShard(String shardId, String sourceHostId, String targetHostId) {
        logger.info("Moving shard {} from host {} to host {}", shardId, sourceHostId, targetHostId);
        
        Map<String, ShardAssignment> shardAssignments = assignments.get(shardId);
        if (shardAssignments == null) {
            logger.warn("No assignments found for shard {}", shardId);
            return;
        }
        
        ShardAssignment sourceAssignment = shardAssignments.get(sourceHostId);
        if (sourceAssignment == null) {
            logger.warn("No assignment found for shard {} on host {}", shardId, sourceHostId);
            return;
        }
        
        // Create a new assignment for the target host
        ShardAssignment targetAssignment = new ShardAssignment(
                shardId,
                targetHostId,
                sourceAssignment.getRole()
        );
        
        // Update the shard state to MOVING
        Shard shard = shards.get(shardId);
        if (shard != null) {
            shard.setState(ShardState.MOVING);
            metadataStore.saveShard(shard);
        }
        <boltAction type="file" filePath="src/main/java/com/meta/shardmanager/core/ShardManager.java">
        // Add the new assignment
        shardAssignments.put(targetHostId, targetAssignment);
        metadataStore.saveAssignment(targetAssignment);
        
        // Update the target host
        Host targetHost = hosts.get(targetHostId);
        if (targetHost != null) {
            targetHost.assignShard(shardId);
            metadataStore.saveHost(targetHost);
        }
        
        // Once the data is copied (in a real system, this would be async)
        // we can remove the old assignment
        shardAssignments.remove(sourceHostId);
        metadataStore.deleteAssignment(sourceAssignment);
        
        // Update the source host
        Host sourceHost = hosts.get(sourceHostId);
        if (sourceHost != null) {
            sourceHost.unassignShard(shardId);
            metadataStore.saveHost(sourceHost);
        }
        
        // Update the shard state back to ACTIVE
        if (shard != null) {
            shard.setState(ShardState.ACTIVE);
            metadataStore.saveShard(shard);
        }
        
        logger.info("Completed moving shard {} from host {} to host {}", 
                shardId, sourceHostId, targetHostId);
    }
    
    public void splitShard(String shardId, List<ShardRange> newRanges) {
        logger.info("Splitting shard {} into {} new shards", shardId, newRanges.size());
        
        Shard originalShard = shards.get(shardId);
        if (originalShard == null) {
            logger.warn("Cannot split shard {}: shard not found", shardId);
            return;
        }
        
        // Mark the original shard as splitting
        originalShard.setState(ShardState.SPLITTING);
        metadataStore.saveShard(originalShard);
        
        // Create new shards
        List<Shard> newShards = new ArrayList<>();
        for (ShardRange range : newRanges) {
            Shard newShard = new Shard(range);
            shards.put(newShard.getId(), newShard);
            metadataStore.saveShard(newShard);
            newShards.add(newShard);
        }
        
        // Assign the new shards to hosts
        for (Shard newShard : newShards) {
            assignShard(newShard);
        }
        
        // Once the data is copied (in a real system, this would be async)
        // we can remove the original shard
        shards.remove(shardId);
        metadataStore.deleteShard(originalShard);
        
        // Remove assignments for the original shard
        Map<String, ShardAssignment> originalAssignments = assignments.remove(shardId);
        if (originalAssignments != null) {
            for (ShardAssignment assignment : originalAssignments.values()) {
                metadataStore.deleteAssignment(assignment);
                
                // Update the host
                Host host = hosts.get(assignment.getHostId());
                if (host != null) {
                    host.unassignShard(shardId);
                    metadataStore.saveHost(host);
                }
            }
        }
        
        logger.info("Completed splitting shard {} into {} new shards", 
                shardId, newShards.size());
    }
    
    public void mergeShard(List<String> shardIds, ShardRange newRange) {
        logger.info("Merging {} shards into a new shard", shardIds.size());
        
        // Validate that all shards exist
        List<Shard> shardsToMerge = new ArrayList<>();
        for (String shardId : shardIds) {
            Shard shard = shards.get(shardId);
            if (shard == null) {
                logger.warn("Cannot merge shard {}: shard not found", shardId);
                return;
            }
            shardsToMerge.add(shard);
        }
        
        // Mark all shards as merging
        for (Shard shard : shardsToMerge) {
            shard.setState(ShardState.MERGING);
            metadataStore.saveShard(shard);
        }
        
        // Create the new merged shard
        Shard mergedShard = new Shard(newRange);
        shards.put(mergedShard.getId(), mergedShard);
        metadataStore.saveShard(mergedShard);
        
        // Assign the new shard to hosts
        assignShard(mergedShard);
        
        // Once the data is copied (in a real system, this would be async)
        // we can remove the original shards
        for (Shard shard : shardsToMerge) {
            shards.remove(shard.getId());
            metadataStore.deleteShard(shard);
            
            // Remove assignments for the original shard
            Map<String, ShardAssignment> originalAssignments = assignments.remove(shard.getId());
            if (originalAssignments != null) {
                for (ShardAssignment assignment : originalAssignments.values()) {
                    metadataStore.deleteAssignment(assignment);
                    
                    // Update the host
                    Host host = hosts.get(assignment.getHostId());
                    if (host != null) {
                        host.unassignShard(shard.getId());
                        metadataStore.saveHost(host);
                    }
                }
            }
        }
        
        logger.info("Completed merging {} shards into new shard {}", 
                shardIds.size(), mergedShard.getId());
    }
    
    public Map<String, Shard> getShards() {
        return Collections.unmodifiableMap(shards);
    }
    
    public Map<String, Host> getHosts() {
        return Collections.unmodifiableMap(hosts);
    }
    
    public Map<String, Map<String, ShardAssignment>> getAssignments() {
        return Collections.unmodifiableMap(assignments);
    }
    
    public void shutdown() {
        logger.info("Shutting down Shard Manager");
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    
    // Method to find the host responsible for a specific key
    public Host getHostForKey(long key) {
        // Find the shard that contains this key
        Optional<Shard> shardOpt = shards.values().stream()
                .filter(s -> s.getRange().contains(key) && s.getState() == ShardState.ACTIVE)
                .findFirst();
        
        if (shardOpt.isPresent()) {
            String shardId = shardOpt.get().getId();
            Map<String, ShardAssignment> shardAssignments = assignments.get(shardId);
            
            if (shardAssignments != null) {
                // Find the primary host for this shard
                Optional<ShardAssignment> primaryOpt = shardAssignments.values().stream()
                        .filter(a -> a.getRole() == ShardAssignment.AssignmentRole.PRIMARY && 
                                a.getState() == ShardAssignment.AssignmentState.ACTIVE)
                        .findFirst();
                
                if (primaryOpt.isPresent()) {
                    return hosts.get(primaryOpt.get().getHostId());
                }
            }
        }
        
        return null;
    }
}
