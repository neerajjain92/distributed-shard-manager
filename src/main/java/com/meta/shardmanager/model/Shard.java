package com.meta.shardmanager.model;

import java.util.Objects;
import java.util.UUID;

/**
 * Represents a shard, which is a unit of data partitioning.
 */
public class Shard {
    private final String id;
    private final ShardRange range;
    private ShardState state;
    
    public Shard(ShardRange range) {
        this.id = UUID.randomUUID().toString();
        this.range = range;
        this.state = ShardState.UNASSIGNED;
    }
    
    public Shard(String id, ShardRange range, ShardState state) {
        this.id = id;
        this.range = range;
        this.state = state;
    }
    
    public String getId() {
        return id;
    }
    
    public ShardRange getRange() {
        return range;
    }
    
    public ShardState getState() {
        return state;
    }
    
    public void setState(ShardState state) {
        this.state = state;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Shard shard = (Shard) o;
        return Objects.equals(id, shard.id);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
    
    @Override
    public String toString() {
        return "Shard{" +
                "id='" + id + '\'' +
                ", range=" + range +
                ", state=" + state +
                '}';
    }
}
