package com.meta.shardmanager.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.meta.shardmanager.model.Host;
import com.meta.shardmanager.model.Shard;
import com.meta.shardmanager.model.ShardAssignment;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of MetadataStore using ZooKeeper.
 */
public class ZookeeperMetadataStore implements MetadataStore {
    private static final Logger logger = LoggerFactory.getLogger(ZookeeperMetadataStore.class);
    
    private final CuratorFramework client;
    private final ObjectMapper objectMapper;
    private final String basePath;
    
    public ZookeeperMetadataStore(String zkConnectString, String basePath) {
        this.basePath = basePath;
        this.objectMapper = new ObjectMapper();
        
        // Initialize ZooKeeper client
        client = CuratorFrameworkFactory.newClient(
                zkConnectString,
                new ExponentialBackoffRetry(1000, 3));
        client.start();
        
        // Ensure base paths exist
        try {
            createPathIfNotExists(basePath);
            createPathIfNotExists(basePath + "/shards");
            createPathIfNotExists(basePath + "/hosts");
            createPathIfNotExists(basePath + "/assignments");
        } catch (Exception e) {
            logger.error("Failed to initialize ZooKeeper paths", e);
            throw new RuntimeException("Failed to initialize ZooKeeper paths", e);
        }
    }
    
    private void createPathIfNotExists(String path) throws Exception {
        try {
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(path);
        } catch (KeeperException.NodeExistsException e) {
            // Path already exists, which is fine
        }
    }
    
    @Override
    public List<Shard> getShards() {
        List<Shard> shards = new ArrayList<>();
        try {
            List<String> shardIds = client.getChildren().forPath(basePath + "/shards");
            for (String shardId : shardIds) {
                Shard shard = getShard(shardId);
                if (shard != null) {
                    shards.add(shard);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to get shards", e);
        }
        return shards;
    }
    
    @Override
    public Shard getShard(String shardId) {
        try {
            byte[] data = client.getData().forPath(basePath + "/shards/" + shardId);
            return objectMapper.readValue(data, Shard.class);
        } catch (Exception e) {
            logger.error("Failed to get shard: {}", shardId, e);
            return null;
        }
    }
    
    @Override
    public void saveShard(Shard shard) {
        try {
            byte[] data = objectMapper.writeValueAsBytes(shard);
            String path = basePath + "/shards/" + shard.getId();
            
            try {
                client.setData().forPath(path, data);
            } catch (KeeperException.NoNodeException e) {
                client.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(path, data);
            }
        } catch (Exception e) {
            logger.error("Failed to save shard: {}", shard.getId(), e);
        }
    }
    
    @Override
    public void deleteShard(Shard shard) {
        try {
            client.delete().forPath(basePath + "/shards/" + shard.getId());
        } catch (Exception e) {
            logger.error("Failed to delete shard: {}", shard.getId(), e);
        }
    }
    
    @Override
    public List<Host> getHosts() {
        List<Host> hosts = new ArrayList<>();
        try {
            List<String> hostIds = client.getChildren().forPath(basePath + "/hosts");
            for (String hostId : hostIds) {
                Host host = getHost(hostId);
                if (host != null) {
                    hosts.add(host);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to get hosts", e);
        }
        return hosts;
    }
    
    @Override
    public Host getHost(String hostId) {
        try {
            byte[] data = client.getData().forPath(basePath + "/hosts/" + hostId);
            return objectMapper.readValue(data, Host.class);
        } catch (Exception e) {
            logger.error("Failed to get host: {}", hostId, e);
            return null;
        }
    }
    
    @Override
    public void saveHost(Host host) {
        try {
            byte[] data = objectMapper.writeValueAsBytes(host);
            String path = basePath + "/hosts/" + host.getId();
            
            try {
                client.setData().forPath(path, data);
            } catch (KeeperException.NoNodeException e) {
                client.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(path, data);
            }
        } catch (Exception e) {
            logger.error("Failed to save host: {}", host.getId(), e);
        }
    }
    
    @Override
    public void deleteHost(Host host) {
        try {
            client.delete().forPath(basePath + "/hosts/" + host.getId());
        } catch (Exception e) {
            logger.error("Failed to delete host: {}", host.getId(), e);
        }
    }
    
    @Override
    public List<ShardAssignment> getAssignments() {
        List<ShardAssignment> assignments = new ArrayList<>();
        try {
            List<String> assignmentPaths = client.getChildren().forPath(basePath + "/assignments");
            for (String path : assignmentPaths) {
                byte[] data = client.getData().forPath(basePath + "/assignments/" + path);
                ShardAssignment assignment = objectMapper.readValue(data, ShardAssignment.class);
                assignments.add(assignment);
            }
        } catch (Exception e) {
            logger.error("Failed to get assignments", e);
        }
        return assignments;
    }
    
    @Override
    public List<ShardAssignment> getAssignmentsForShard(String shardId) {
        List<ShardAssignment> assignments = new ArrayList<>();
        try {
            String shardPath = basePath + "/assignments/by-shard/" + shardId;
            if (client.checkExists().forPath(shardPath) != null) {
                List<String> hostIds = client.getChildren().forPath(shardPath);
                for (String hostId : hostIds) {
                    byte[] data = client.getData().forPath(shardPath + "/" + hostId);
                    ShardAssignment assignment = objectMapper.readValue(data, ShardAssignment.class);
                    assignments.add(assignment);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to get assignments for shard: {}", shardId, e);
        }
        return assignments;
    }
    
    @Override
    public List<ShardAssignment> getAssignmentsForHost(String hostId) {
        List<ShardAssignment> assignments = new ArrayList<>();
        try {
            String hostPath = basePath + "/assignments/by-host/" + hostId;
            if (client.checkExists().forPath(hostPath) != null) {
                List<String> shardIds = client.getChildren().forPath(hostPath);
                for (String shardId : shardIds) {
                    byte[] data = client.getData().forPath(hostPath + "/" + shardId);
                    ShardAssignment assignment = objectMapper.readValue(data, ShardAssignment.class);
                    assignments.add(assignment);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to get assignments for host: {}", hostId, e);
        }
        return assignments;
    }
    
    @Override
    public void saveAssignment(ShardAssignment assignment) {
        try {
            byte[] data = objectMapper.writeValueAsBytes(assignment);
            
            // Save in the main assignments directory
            String mainPath = basePath + "/assignments/" + 
                    assignment.getShardId() + "-" + assignment.getHostId();
            try {
                client.setData().forPath(mainPath, data);
            } catch (KeeperException.NoNodeException e) {
                client.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(mainPath, data);
            }
            
            // Save in the by-shard directory
            String byShardPath = basePath + "/assignments/by-shard/" + 
                    assignment.getShardId() + "/" + assignment.getHostId();
            try {
                client.setData().forPath(byShardPath, data);
            } catch (KeeperException.NoNodeException e) {
                client.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(byShardPath, data);
            }
            
            // Save in the by-host directory
            String byHostPath = basePath + "/assignments/by-host/" + 
                    assignment.getHostId() + "/" + assignment.getShardId();
            try {
                client.setData().forPath(byHostPath, data);
            } catch (KeeperException.NoNodeException e) {
                client.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(byHostPath, data);
            }
        } catch (Exception e) {
            logger.error("Failed to save assignment: {} -> {}", 
                    assignment.getShardId(), assignment.getHostId(), e);
        }
    }
    
    @Override
    public void deleteAssignment(ShardAssignment assignment) {
        try {
            // Delete from main assignments directory
            String mainPath = basePath + "/assignments/" + 
                    assignment.getShardId() + "-" + assignment.getHostId();
            client.delete().forPath(mainPath);
            
            // Delete from by-shard directory
            String byShardPath = basePath + "/assignments/by-shard/" + 
                    assignment.getShardId() + "/" + assignment.getHostId();
            client.delete().forPath(byShardPath);
            
            // Delete from by-host directory
            String byHostPath = basePath + "/assignments/by-host/" + 
                    assignment.getHostId() + "/" + assignment.getShardId();
            client.delete().forPath(byHostPath);
        } catch (Exception e) {
            logger.error("Failed to delete assignment: {} -> {}", 
                    assignment.getShardId(), assignment.getHostId(), e);
        }
    }
    
    public void close() {
        client.close();
    }
}
