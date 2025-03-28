package com.meta.shardmanager.model;

import java.util.Objects;

/**
 * Represents an assignment of a shard to a host.
 */
public class ShardAssignment {
    private final String shardId;
    private final String hostId;
    private final AssignmentRole role;
    private AssignmentState state;
    
    public ShardAssignment(String shardId, String hostId, AssignmentRole role) {
        this.shardId = shardId;
        this.hostId = hostId;
        this.role = role;
        this.state = AssignmentState.PENDING;
    }
    
    public ShardAssignment(String shardId, String hostId, AssignmentRole role, AssignmentState state) {
        this.shardId = shardId;
        this.hostId = hostId;
        this.role = role;
        this.state = state;
    }
    
    public String getShardId() {
        return shardId;
    }
    
    public String getHostId() {
        return hostId;
    }
    
    public AssignmentRole getRole() {
        return role;
    }
    
    public AssignmentState getState() {
        return state;
    }
    
    public void setState(AssignmentState state) {
        this.state = state;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardAssignment that = (ShardAssignment) o;
        return Objects.equals(shardId, that.shardId) &&
                Objects.equals(hostId, that.hostId) &&
                role == that.role;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(shardId, hostId, role);
    }
    
    @Override
    public String toString() {
        return "ShardAssignment{" +
                "shardId='" + shardId + '\'' +
                ", hostId='" + hostId + '\'' +
                ", role=" + role +
                ", state=" + state +
                '}';
    }
    
    public enum AssignmentRole {
        PRIMARY,
        SECONDARY,
        STANDBY
    }
    
    public enum AssignmentState {
        PENDING,
        ASSIGNED,
        ACTIVE,
        FAILED,
        DECOMMISSIONING
    }
}
