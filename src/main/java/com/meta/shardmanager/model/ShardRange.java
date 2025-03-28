package com.meta.shardmanager.model;

/**
 * Represents a range of keys that a shard is responsible for.
 */
public class ShardRange {
    private final long startKey;
    private final long endKey;
    
    public ShardRange(long startKey, long endKey) {
        if (startKey >= endKey) {
            throw new IllegalArgumentException("Start key must be less than end key");
        }
        this.startKey = startKey;
        this.endKey = endKey;
    }
    
    public long getStartKey() {
        return startKey;
    }
    
    public long getEndKey() {
        return endKey;
    }
    
    public boolean contains(long key) {
        return key >= startKey && key < endKey;
    }
    
    @Override
    public String toString() {
        return "ShardRange{" +
                "startKey=" + startKey +
                ", endKey=" + endKey +
                '}';
    }
}
