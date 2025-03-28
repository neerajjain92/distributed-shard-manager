package com.meta.shardmanager.core;

import com.meta.shardmanager.model.Host;
import com.meta.shardmanager.model.HostState;
import com.meta.shardmanager.model.ShardAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages failover and failback of hosts and shards.
 */
public class FailoverManager {
    private static final Logger logger = LoggerFactory.getLogger(FailoverManager.class);
    
    private final ShardManager shardManager;
    private final Map<String, AtomicInteger> hostFailureCounters = new ConcurrentHashMap<>();
    private final int failureThreshold = 3; // Number of consecutive failures before marking a host as down
    private final int connectionTimeout = 2000; // Connection timeout in milliseconds
    
    public FailoverManager(ShardManager shardManager) {
        this.shardManager = shardManager;
    }
    
    public boolean checkHostHealth(Host host) {
        if (host.getState() == HostState.MAINTENANCE || host.getState() == HostState.DRAINING) {
            // Don't check hosts that are in maintenance or draining
            return false;
        }
        
        // In a real system, this would be a more sophisticated health check
        // For this example, we'll just try to connect to the host's port
        boolean isHealthy = isHostReachable(host.getHostname(), host.getPort());
        
        AtomicInteger failureCounter = hostFailureCounters.computeIfAbsent(
                host.getId(), k -> new AtomicInteger(0));
        
        if (isHealthy) {
            // Reset failure counter on success
            failureCounter.set(0);
            return true;
        } else {
            // Increment failure counter
            int failures = failureCounter.incrementAndGet();
            logger.warn("Host {} health check failed ({}/{})", 
                    host.getId(), failures, failureThreshold);
            
            // Only consider a host down after multiple consecutive failures
            return failures < failureThreshold;
        }
    }
    
    private boolean isHostReachable(String hostname, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(hostname, port), connectionTimeout);
            return true;
        } catch (SocketTimeoutException e) {
            logger.debug("Connection to {}:{} timed out", hostname, port);
            return false;
        } catch (IOException e) {
            logger.debug("Failed to connect to {}:{}: {}", hostname, port, e.getMessage());
            return false;
        }
    }
    
    public void handleHostFailure(String hostId) {
        logger.info("Handling failure of host: {}", hostId);
        
        // Get all shard assignments for this host
        Map<String, Map<String, ShardAssignment>> allAssignments = shardManager.getAssignments();
        
        for (Map.Entry<String, Map<String, ShardAssignment>> entry : allAssignments.entrySet()) {
            String shardId = entry.getKey();
            Map<String, ShardAssignment> shardAssignments = entry.getValue();
            
            ShardAssignment assignment = shardAssignments.get(hostId);
            if (assignment != null) {
                // Handle based on the role
                if (assignment.getRole() == ShardAssignment.AssignmentRole.PRIMARY) {
                    logger.info("Initiating failover for primary shard {} on failed host {}", 
                            shardId, hostId);
                    
                    // In a real system, this would trigger a more complex failover process
                    // For this example, we'll just reassign the shard
                    shardManager.reassignShardsFromHost(hostId);
                } else {
                    logger.info("Marking secondary/standby assignment as failed for shard {} on host {}", 
                            shardId, hostId);
                    
                    // Mark the assignment as failed
                    assignment.setState(ShardAssignment.AssignmentState.FAILED);
                    
                    // In a real system, we would also trigger creation of a new replica
                }
            }
        }
    }
    
    public void handleHostRecovery(String hostId) {
        logger.info("Handling recovery of host: {}", hostId);
        
        // Reset failure counter
        hostFailureCounters.computeIfAbsent(hostId, k -> new AtomicInteger(0)).set(0);
        
        // In a real system, we would:
        // 1. Verify the host is fully operational
        // 2. Sync any missed data
        // 3. Gradually reintroduce the host to the cluster
        
        // For this example, we'll just mark the host as online
        // and let the rebalancer assign shards to it
        Host host = shardManager.getHosts().get(hostId);
        if (host != null && host.getState() == HostState.OFFLINE) {
            host.setState(HostState.ONLINE);
            logger.info("Host {} is now back online", hostId);
        }
    }
}
