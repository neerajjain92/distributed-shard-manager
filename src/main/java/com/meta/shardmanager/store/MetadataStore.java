package com.meta.shardmanager.store;

import com.meta.shardmanager.model.Host;
import com.meta.shardmanager.model.Shard;
import com.meta.shardmanager.model.ShardAssignment;

import java.util.List;

/**
 * Interface for storing and retrieving metadata about shards, hosts, and assignments.
 */
public interface MetadataStore {
    
    // Shard operations
    List<Shard> getShards();
    Shard getShard(String shardId);
    void saveShard(Shard shard);
    void deleteShard(Shard shard);
    
    // Host operations
    List<Host> getHosts();
    Host getHost(String hostId);
    void saveHost(Host host);
    void deleteHost(Host host);
    
    // Assignment operations
    List<ShardAssignment> getAssignments();
    List<ShardAssignment> getAssignmentsForShard(String shardId);
    List<ShardAssignment> getAssignmentsForHost(String hostId);
    void saveAssignment(ShardAssignment assignment);
    void deleteAssignment(ShardAssignment assignment);
}
