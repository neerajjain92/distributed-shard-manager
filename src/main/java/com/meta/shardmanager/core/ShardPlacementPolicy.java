package com.meta.shardmanager.core;

import com.meta.shardmanager.model.Host;
import com.meta.shardmanager.model.HostState;
import com.meta.shardmanager.model.Shard;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Defines policies for placing shards on hosts.
 */
public class ShardPlacementPolicy {
    
    /**
     * Selects hosts for a new shard based on load balancing and fault domain awareness.
     */
    public List<Host> selectHostsForShard(Shard shard, List<Host> availableHosts, int replicationFactor) {
        // Filter out hosts that are not online or don't have capacity
        List<Host> eligibleHosts = availableHosts.stream()
                .filter(h -> h.getState() == HostState.ONLINE && h.hasCapacity())
                .collect(Collectors.toList());
        
        if (eligibleHosts.size() < replicationFactor) {
            // Not enough eligible hosts
            return eligibleHosts;
        }
        
        // Sort hosts by load (least loaded first)
        eligibleHosts.sort(Comparator.comparingDouble(Host::getLoad));
        
        // In a real system, we would also consider:
        // 1. Fault domains (rack, datacenter, etc.)
        // 2. Network topology
        // 3. Hardware capabilities
        // 4. Current shard distribution
        
        // For this example, we'll just take the least loaded hosts
        return eligibleHosts.subList(0, replicationFactor);
    }
    
    /**
     * Selects a new host for a shard that needs to be reassigned.
     */
    public Host selectNewHostForShard(Shard shard, List<Host> availableHosts, List<Host> currentHosts) {
        // Filter out hosts that are not online, don't have capacity, or already have this shard
        Set<String> currentHostIds = currentHosts.stream()
                .map(Host::getId)
                .collect(Collectors.toSet());
        
        List<Host> eligibleHosts = availableHosts.stream()
                .filter(h -> h.getState() == HostState.ONLINE && 
                        h.hasCapacity() && 
                        !currentHostIds.contains(h.getId()))
                .collect(Collectors.toList());
        
        if (eligibleHosts.isEmpty()) {
            return null;
        }
        
        // Sort hosts by load (least loaded first)
        eligibleHosts.sort(Comparator.comparingDouble(Host::getLoad));
        
        // Return the least loaded host
        return eligibleHosts.get(0);
    }
}
