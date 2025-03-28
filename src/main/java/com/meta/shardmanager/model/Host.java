package com.meta.shardmanager.model;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a host machine that can serve shards.
 */
public class Host {
    private final String id;
    private final String hostname;
    private final int port;
    private HostState state;
    private final Set<String> assignedShardIds;
    private double load;
    private final int capacity;
    
    public Host(String id, String hostname, int port, int capacity) {
        this.id = id;
        this.hostname = hostname;
        this.port = port;
        this.state = HostState.ONLINE;
        this.assignedShardIds = new HashSet<>();
        this.load = 0.0;
        this.capacity = capacity;
    }
    
    public String getId() {
        return id;
    }
    
    public String getHostname() {
        return hostname;
    }
    
    public int getPort() {
        return port;
    }
    
    public HostState getState() {
        return state;
    }
    
    public void setState(HostState state) {
        this.state = state;
    }
    
    public Set<String> getAssignedShardIds() {
        return new HashSet<>(assignedShardIds);
    }
    
    public boolean assignShard(String shardId) {
        if (assignedShardIds.size() >= capacity) {
            return false;
        }
        return assignedShardIds.add(shardId);
    }
    
    public boolean unassignShard(String shardId) {
        return assignedShardIds.remove(shardId);
    }
    
    public double getLoad() {
        return load;
    }
    
    public void setLoad(double load) {
        this.load = load;
    }
    
    public int getCapacity() {
        return capacity;
    }
    
    public boolean hasCapacity() {
        return assignedShardIds.size() < capacity;
    }
    
    public double getUtilization() {
        return assignedShardIds.size() / (double) capacity;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Host host = (Host) o;
        return Objects.equals(id, host.id);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
    
    @Override
    public String toString() {
        return "Host{" +
                "id='" + id + '\'' +
                ", hostname='" + hostname + '\'' +
                ", port=" + port +
                ", state=" + state +
                ", assignedShards=" + assignedShardIds.size() +
                ", load=" + load +
                ", capacity=" + capacity +
                '}';
    }
}
