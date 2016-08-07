package com.bloom.runtime;

import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.uuid.UUID;

import java.util.ArrayList;
import java.util.List;

public class SimplePartitionManager
  implements IPartitionManager
{
  private final UUID thisID;
  private ConsistentHashRing ring;
  
  public SimplePartitionManager(String cacheName, int numReplicas)
  {
    this.ring = new ConsistentHashRing(cacheName, numReplicas);
    this.thisID = HazelcastSingleton.getNodeId();
  }
  
  public SimplePartitionManager(String cacheName, int numReplicas, int numPartitions)
  {
    this.ring = new ConsistentHashRing(cacheName, numReplicas, numPartitions);
    this.thisID = HazelcastSingleton.getNodeId();
  }
  
  public void doShutdown() {}
  
  public void setServers(List<UUID> servers)
  {
    this.ring.set(servers);
  }
  
  public int getNumReplicas()
  {
    return this.ring.getNumReplicas();
  }
  
  public int getPartitionId(Object key)
  {
    return this.ring.getPartitionId(key);
  }
  
  public UUID getPartitionOwnerForKey(Object key, int n)
  {
    return this.ring.getUUIDForKey(key, n);
  }
  
  public UUID getPartitionOwnerForPartition(int partId, int n)
  {
    return this.ring.getUUIDForPartition(partId, n);
  }
  
  public UUID getFirstPartitionOwnerForPartition(int partId)
  {
    return this.ring.getUUIDForPartition(partId, 0);
  }
  
  public boolean isLocalPartitionByKey(Object key, int n)
  {
    return this.thisID.equals(getPartitionOwnerForKey(key, n));
  }
  
  public boolean isLocalPartitionById(int partId, int n)
  {
    return this.thisID.equals(getPartitionOwnerForPartition(partId, n));
  }
  
  public boolean hasLocalPartitionForId(int partId)
  {
    if (this.ring.isFullyReplicated()) {
      return true;
    }
    for (int i = 0; i < this.ring.getNumReplicas(); i++) {
      if (isLocalPartitionById(partId, i)) {
        return true;
      }
    }
    return false;
  }
  
  public boolean hasLocalPartitionForKey(Object key)
  {
    int partId = getPartitionId(key);
    return hasLocalPartitionForId(partId);
  }
  
  public List<UUID> getAllReplicas(int partId)
  {
    List<UUID> ret = new ArrayList();
    for (int i = 0; i < this.ring.getNumReplicas(); i++)
    {
      UUID uuid = getPartitionOwnerForPartition(partId, i);
      if (!ret.contains(uuid)) {
        ret.add(uuid);
      }
    }
    return ret;
  }
  
  public boolean isLocalPartitionByUUID(UUID uuid)
  {
    return this.thisID.equals(uuid);
  }
  
  public UUID getLocal()
  {
    return this.thisID;
  }
  
  public boolean isMultiNode()
  {
    return this.ring.isMultiNode();
  }
  
  public List<UUID> getPeers()
  {
    return new ArrayList(this.ring.getNodes());
  }
  
  public void addUpdateListener(ConsistentHashRing.UpdateListener listener)
  {
    this.ring.addUpdateListener(listener);
  }
  
  public int getNumberOfPartitions()
  {
    return this.ring.getNumberOfPartitions();
  }
}
