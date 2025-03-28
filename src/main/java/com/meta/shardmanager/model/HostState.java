package com.meta.shardmanager.model;

/**
 * Represents the possible states of a host.
 */
public enum HostState {
    // Host is online and can serve requests
    ONLINE,
    
    // Host is online but not accepting new shards
    DRAINING,
    
    // Host is offline and cannot serve requests
    OFFLINE,
    
    // Host is in maintenance mode
    MAINTENANCE,
    
    // Host is in the process of being added to the cluster
    JOINING,
    
    // Host is in the process of being removed from the cluster
    LEAVING
}
