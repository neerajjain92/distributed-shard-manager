package com.meta.shardmanager.model;

/**
 * Represents the possible states of a shard.
 */
public enum ShardState {
    // Shard is not assigned to any host
    UNASSIGNED,
    
    // Shard is assigned but not yet active
    ASSIGNED,
    
    // Shard is active and serving requests
    ACTIVE,
    
    // Shard is being moved to another host
    MOVING,
    
    // Shard is being split into multiple shards
    SPLITTING,
    
    // Shard is being merged with another shard
    MERGING,
    
    // Shard is in recovery mode after a failure
    RECOVERING,
    
    // Shard is read-only (e.g., during maintenance)
    READ_ONLY,
    
    // Shard is offline and not serving requests
    OFFLINE
}
