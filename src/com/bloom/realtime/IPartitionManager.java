package com.bloom.runtime;

import java.util.List;

import com.bloom.uuid.UUID;

public abstract interface IPartitionManager
{
  public abstract void doShutdown();
  
  public abstract void setServers(List<UUID> paramList);
  
  public abstract int getNumReplicas();
  
  public abstract int getPartitionId(Object paramObject);
  
  public abstract UUID getPartitionOwnerForKey(Object paramObject, int paramInt);
  
  public abstract UUID getPartitionOwnerForPartition(int paramInt1, int paramInt2);
  
  public abstract UUID getFirstPartitionOwnerForPartition(int paramInt);
  
  public abstract boolean isLocalPartitionByKey(Object paramObject, int paramInt);
  
  public abstract boolean isLocalPartitionById(int paramInt1, int paramInt2);
  
  public abstract boolean hasLocalPartitionForId(int paramInt);
  
  public abstract boolean hasLocalPartitionForKey(Object paramObject);
  
  public abstract List<UUID> getAllReplicas(int paramInt);
  
  public abstract boolean isLocalPartitionByUUID(UUID paramUUID);
  
  public abstract UUID getLocal();
  
  public abstract boolean isMultiNode();
  
  public abstract List<UUID> getPeers();
  
  public abstract void addUpdateListener(ConsistentHashRing.UpdateListener paramUpdateListener);
  
  public abstract int getNumberOfPartitions();
}

