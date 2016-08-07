package com.bloom.runtime;

import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.runtime.exceptions.NodeNotFoundException;
import com.bloom.runtime.utils.Factory;
import com.bloom.uuid.UUID;
import com.hazelcast.core.Client;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Endpoint;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.bloom.distribution.Partitionable;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.log4j.Logger;

public class ConsistentHashRing
{
  private static Logger logger = Logger.getLogger(ConsistentHashRing.class);
  private static final int NULL_HASH_CODE = "NULL".hashCode();
  private static final int numberOfPieces = 503;
  private final int requestedNumberOfReplicas;
  private int numberOfReplicas;
  private final int numberOfPartitions;
  private final String name;
  
  public int murmur(byte[] data, int seed)
  {
    int m = 1540483477;
    int r = 24;
    int h = seed ^ data.length;
    int len = data.length;
    int len_4 = len >> 2;
    for (int i = 0; i < len_4; i++)
    {
      int i_4 = i << 2;
      int k = data[(i_4 + 3)];
      k <<= 8;
      k |= data[(i_4 + 2)] & 0xFF;
      k <<= 8;
      k |= data[(i_4 + 1)] & 0xFF;
      k <<= 8;
      k |= data[(i_4 + 0)] & 0xFF;
      k *= m;
      k ^= k >>> r;
      k *= m;
      h *= m;
      h ^= k;
    }
    int len_m = len_4 << 2;
    int left = len - len_m;
    if (left != 0)
    {
      if (left >= 3) {
        h ^= data[(len - 3)] << 16;
      }
      if (left >= 2) {
        h ^= data[(len - 2)] << 8;
      }
      if (left >= 1) {
        h ^= data[(len - 1)];
      }
      h *= m;
    }
    h ^= h >>> 13;
    h *= m;
    h ^= h >>> 15;
    return h;
  }
  
  public int goldenratio32(int i)
  {
    return i * -1640531527;
  }
  
  public int hash(Object obj)
  {
    if ((obj instanceof String)) {
      return murmur(((String)obj).getBytes(), 503);
    }
    return goldenratio32(obj.hashCode());
  }
  
  private final TreeMap<Integer, UUID> ring = new TreeMap();
  private final List<UUID> currentNodes = new ArrayList();
  private final UUID thisId = HazelcastSingleton.getNodeId();
  private final Map<UUID, String> addrCache = Factory.makeMap();
  
  private List<Pair<String, String>> getNode2IpMap()
  {
    List<Pair<String, String>> ret = new ArrayList();
    HazelcastInstance hz = HazelcastSingleton.get();
    for (Member m : hz.getCluster().getMembers())
    {
      String uuid = m.getUuid();
      InetSocketAddress a = m.getSocketAddress();
      String ip = a.getAddress().getHostAddress() + ":" + a.getPort();
      ret.add(Pair.make(uuid, ip));
    }
    return ret;
  }
  
  private String idToAddress(UUID uuid)
    throws NodeNotFoundException
  {
    String addr = (String)this.addrCache.get(uuid);
    if (addr == null)
    {
      Endpoint member = (Endpoint)HazelcastSingleton.getActiveMembersMap().get(uuid);
      if (member == null)
      {
        HazelcastInstance hz = HazelcastSingleton.get();
        for (Member m : hz.getCluster().getMembers()) {
          if (m.getUuid().equals(uuid.toString()))
          {
            member = m;
            break;
          }
        }
      }
      if (member != null)
      {
        InetSocketAddress a = null;
        if ((member instanceof Member)) {
          a = ((Member)member).getSocketAddress();
        } else if ((member instanceof Client)) {
          a = ((Client)member).getSocketAddress();
        }
        addr = a.getAddress().getHostAddress() + ":" + a.getPort();
        this.addrCache.put(uuid, addr);
        return addr;
      }
    }
    else
    {
      return addr;
    }
    if (HazelcastSingleton.isClientMember()) {
      return uuid.toString();
    }
    throw new NodeNotFoundException("cannot map node ID [" + uuid + "] to socket address\n" + getNode2IpMap());
  }
  
  public ConsistentHashRing()
  {
    this("HashRing-" + System.currentTimeMillis(), 1);
  }
  
  public ConsistentHashRing(String name, int requestedNumberOfReplicas)
  {
    this(name, requestedNumberOfReplicas, 1023);
  }
  
  public ConsistentHashRing(String name, int requestedNumberOfReplicas, int numberOfPartitions)
  {
    this.requestedNumberOfReplicas = requestedNumberOfReplicas;
    this.numberOfReplicas = 0;
    this.name = name;
    this.numberOfPartitions = numberOfPartitions;
    if (logger.isDebugEnabled()) {
      logger.debug(name + ": Created Partitioning scheme with " + this.numberOfReplicas + " replicas");
    }
  }
  
  public String getName()
  {
    return this.name;
  }
  
  private UUID[][] currentParts = (UUID[][])null;
  
  public UUID[][] getPartitionMap()
  {
    if (this.requestedNumberOfReplicas == 0) {
      this.numberOfReplicas = this.currentNodes.size();
    } else {
      this.numberOfReplicas = this.requestedNumberOfReplicas;
    }
    if (this.numberOfReplicas == 0) {
      return (UUID[][])null;
    }
    UUID[][] parts = new UUID[this.numberOfPartitions][];
    synchronized (this.ring)
    {
      for (int p = 0; p < this.numberOfPartitions; p++)
      {
        parts[p] = new UUID[this.numberOfReplicas];
        
        int midValue = p * (Integer.MAX_VALUE / this.numberOfPartitions) + Integer.MAX_VALUE / (this.numberOfPartitions * 2);
        
        List<UUID> uuids = getN(Integer.valueOf(midValue), this.numberOfReplicas);
        assert (uuids != null);
        for (int b = 0; b < this.numberOfReplicas; b++) {
          parts[p][b] = ((UUID)uuids.get(b < uuids.size() ? b : uuids.size() - 1));
        }
      }
    }
    return parts;
  }
  
  public int getNumberOfPartitions()
  {
    return this.numberOfPartitions;
  }
  
  public List<Integer> getPartitionsForNode(UUID uuid)
  {
    List<Integer> partitions = new ArrayList();
    for (Map.Entry<Integer, UUID> entry : this.ring.entrySet()) {
      if (((UUID)entry.getValue()).equals(uuid)) {
        partitions.add(entry.getKey());
      }
    }
    return partitions;
  }
  
  public boolean isMultiNode()
  {
    return this.currentNodes.size() > 1;
  }
  
  public List<UUID> getNodes()
  {
    return this.currentNodes;
  }
  
  public int size()
  {
    return this.currentNodes.size();
  }
  
  public int getNumReplicas()
  {
    return this.numberOfReplicas;
  }
  
  public boolean isFullyReplicated()
  {
    return this.requestedNumberOfReplicas == 0;
  }
  
  public static abstract interface UpdateListener
  {
    public abstract void updateStart();
    
    public abstract void update(ConsistentHashRing.Update paramUpdate);
    
    public abstract void updateEnd();
  }
  
  public static class Update
    implements Serializable
  {
    private static final long serialVersionUID = 2814082967693009878L;
    public UUID from;
    public UUID to;
    public int partId;
    
    public Update() {}
    
    public Update(UUID from, UUID to, int partId)
    {
      this.from = from;
      this.to = to;
      this.partId = partId;
    }
    
    public String toString()
    {
      return "Repartition from " + this.from + " to " + this.to + " partition " + this.partId;
    }
  }
  
  private List<UpdateListener> listeners = new ArrayList();
  
  public void addUpdateListener(UpdateListener listener)
  {
    synchronized (this.listeners)
    {
      this.listeners.add(listener);
    }
  }
  
  public void removeUpdateListener(UpdateListener listener)
  {
    synchronized (this.listeners)
    {
      this.listeners.remove(listener);
    }
  }
  
  private void updateStart()
  {
    if (logger.isInfoEnabled()) {
      logger.info(this.name + ": updating start listeners");
    }
    for (UpdateListener listener : this.listeners) {
      listener.updateStart();
    }
    if (logger.isInfoEnabled()) {
      logger.info(this.name + ": done updating start listeners");
    }
  }
  
  private void updateEnd()
  {
    if (logger.isInfoEnabled()) {
      logger.info(this.name + ": updating end listeners");
    }
    for (UpdateListener listener : this.listeners) {
      listener.updateEnd();
    }
    if (logger.isInfoEnabled()) {
      logger.info(this.name + ": done updating end listeners");
    }
  }
  
  private void update(UUID from, UUID to, int partId)
  {
    Update update = new Update(from, to, partId);
    for (UpdateListener listener : this.listeners) {
      listener.update(update);
    }
  }
  
  public void showDistribution()
  {
    if (this.currentNodes.size() == 0) {
      return;
    }
    if (logger.isDebugEnabled())
    {
      logger.debug(this.name + " Distribution");
      for (int b = 0; b < this.numberOfReplicas; b++)
      {
        logger.debug("  " + (b == 0 ? "Primary" : new StringBuilder().append("Backup ").append(b).toString()) + " Partitions");
        Map<UUID, Integer> counts = new TreeMap();
        for (int p = 0; p < this.numberOfPartitions; p++)
        {
          UUID uuid = this.currentParts[p][b];
          Integer currentCount = (Integer)counts.get(uuid);
          counts.put(uuid, Integer.valueOf(currentCount == null ? 1 : currentCount.intValue() + 1));
        }
        for (Map.Entry<UUID, Integer> count : counts.entrySet()) {
          logger.debug("    " + count.getKey() + " = " + count.getValue());
        }
      }
    }
  }
  
  private boolean shuttingDown = false;
  
  public void shutDown()
  {
    this.shuttingDown = true;
  }
  
  public void repartition()
  {
    if (logger.isInfoEnabled()) {
      logger.info(this.name + ": start Repartitioning data");
    }
    if (this.ring.isEmpty())
    {
      this.currentParts = ((UUID[][])null);
      if (logger.isInfoEnabled()) {
        logger.info(this.name + ": done Repartitioning data");
      }
      return;
    }
    if (this.shuttingDown) {
      return;
    }
    synchronized (this.ring)
    {
      updateStart();
      
      UUID[][] newParts = getPartitionMap();
      
      int numMovements = 0;
      if ((this.currentParts != null) && (newParts != null))
      {
        if (logger.isInfoEnabled()) {
          logger.info(this.name + ": Repartitioning data");
        }
        int oldPercent = 0;
        for (int p = 0; p < this.numberOfPartitions; p++)
        {
          UUID[] oldP = this.currentParts[p];
          UUID[] newP = newParts[p];
          
          List<UUID> oldPList = Arrays.asList(oldP);
          List<UUID> newPList = Arrays.asList(newP);
          for (int iNew = 0; iNew < newP.length; iNew++) {
            if (!oldPList.contains(newP[iNew])) {
              for (int iOld = 0; iOld < oldP.length; iOld++) {
                if (this.currentNodes.contains(oldP[iOld]))
                {
                  if (!this.thisId.equals(oldP[iOld])) {
                    break;
                  }
                  update(oldP[iOld], newP[iNew], p);
                  numMovements++; break;
                }
              }
            }
          }
          this.currentParts[p] = newParts[p];
          int percent = p * 100 / this.numberOfPartitions;
          if ((percent % 20 == 0) && (percent != oldPercent))
          {
            if (logger.isDebugEnabled()) {
              logger.debug("  " + this.name + ": Repartitioning data: " + percent + "%");
            }
            oldPercent = percent;
          }
        }
        if (logger.isInfoEnabled()) {
          logger.info(this.name + ": Repartitioning complete with " + numMovements + " movements");
        }
      }
      else
      {
        assert (newParts != null);
        
        this.currentParts = newParts;
        if (logger.isInfoEnabled()) {
          logger.info(this.name + ": done Repartitioning data");
        }
      }
      updateEnd();
    }
    showDistribution();
  }
  
  public int getPartitionId(Object key)
  {
    if ((key instanceof Partitionable))
    {
      Partitionable p = (Partitionable)key;
      if (p.usePartitionId()) {
        return p.getPartitionId();
      }
      int hashCode = p.getPartitionKey() == null ? NULL_HASH_CODE : p.getPartitionKey().hashCode();
      return Math.abs(hashCode % this.numberOfPartitions);
    }
    return Math.abs((key == null ? NULL_HASH_CODE : key.hashCode()) % this.numberOfPartitions);
  }
  
  public UUID getUUIDForKey(Object key, int n)
  {
    int partId = getPartitionId(key);
    return getUUIDForPartition(partId, n);
  }
  
  public UUID getUUIDForPartition(int partId, int n)
  {
    synchronized (this.ring)
    {
      if (n >= this.numberOfReplicas) {
        throw new IllegalArgumentException("Cannot get a backup partition greater than number of backups: " + n + " >= " + this.numberOfReplicas);
      }
      if (partId >= this.numberOfPartitions) {
        throw new IllegalArgumentException("Cannot access a partition greater than number of partitions: " + partId + " >= " + this.numberOfPartitions);
      }
      if ((this.currentParts == null) || (this.currentParts[partId] == null)) {
        return this.thisId;
      }
      UUID[] prt = this.currentParts[partId];
      UUID partuuid = prt[n];
      return partuuid;
    }
  }
  
  private static void checkPart(UUID[] part)
  {
    assert (part != null);
    int n = part.length;
    for (int i = 0; i < n; i++)
    {
      UUID id = part[i];
      if ((id == null) && 
        (!$assertionsDisabled) && (id == null)) {
        throw new AssertionError();
      }
    }
  }
  
  private static void checkPartsTable(UUID[][] table)
  {
    assert (table != null);
    int n = table.length;
    for (int i = 0; i < n; i++) {
      checkPart(table[i]);
    }
  }
  
  private void addNode(UUID node)
    throws NodeNotFoundException
  {
    String nodeId = idToAddress(node);
    synchronized (this.ring)
    {
      for (int i = 0; i < 503; i++) {
        this.ring.put(Integer.valueOf(hash(nodeId + i)), node);
      }
      this.currentNodes.add(node);
      Set<UUID> allNodes = new HashSet();
      for (Map.Entry<Integer, UUID> entry : this.ring.entrySet()) {
        allNodes.add(entry.getValue());
      }
      if (logger.isInfoEnabled()) {
        logger.info(this.name + ": Added node: " + node + " now " + this.currentNodes);
      }
    }
  }
  
  public void add(UUID node)
    throws NodeNotFoundException
  {
    addNode(node);
    repartition();
  }
  
  private void removeNode(UUID node)
    throws NodeNotFoundException
  {
    String nodeId = idToAddress(node);
    synchronized (this.ring)
    {
      for (int i = 0; i < 503; i++) {
        this.ring.remove(Integer.valueOf(hash(nodeId + i)));
      }
      this.currentNodes.remove(node);
      Set<UUID> allNodes = new HashSet();
      for (Map.Entry<Integer, UUID> entry : this.ring.entrySet()) {
        allNodes.add(entry.getValue());
      }
      if (logger.isInfoEnabled()) {
        logger.info(this.name + ": Removed node: " + node + " now " + this.currentNodes);
      }
    }
  }
  
  public void remove(UUID node)
    throws NodeNotFoundException
  {
    removeNode(node);
    repartition();
  }
  
  public void set(List<UUID> nodes)
  {
    List<UUID> toAdd = new ArrayList();
    List<UUID> toRemove = new ArrayList();
    synchronized (this.ring)
    {
      if ((nodes != null) && (!nodes.isEmpty()))
      {
        for (UUID node : nodes) {
          if (!this.currentNodes.contains(node)) {
            toAdd.add(node);
          }
        }
        for (UUID node : this.currentNodes) {
          if (!nodes.contains(node)) {
            toRemove.add(node);
          }
        }
      }
      else
      {
        toRemove.addAll(this.currentNodes);
      }
      for (UUID node : toAdd) {
        try
        {
          addNode(node);
        }
        catch (NodeNotFoundException e) {}
      }
      for (UUID node : toRemove) {
        try
        {
          removeNode(node);
        }
        catch (NodeNotFoundException e) {}
      }
      if ((toAdd.size() > 0) || (toRemove.size() > 0)) {
        repartition();
      }
    }
  }
  
  public UUID get(Object key)
  {
    if (this.ring.isEmpty()) {
      return null;
    }
    int hash = hash(key);
    SortedMap<Integer, UUID> tailMap = this.ring.tailMap(Integer.valueOf(hash));
    hash = (tailMap.isEmpty() ? (Integer)this.ring.firstKey() : (Integer)tailMap.firstKey()).intValue();
    return (UUID)this.ring.get(Integer.valueOf(hash));
  }
  
  public UUID getNext(Object key)
  {
    if (this.ring.isEmpty()) {
      return null;
    }
    UUID first = get(key);
    int hash = hash(key);
    int origHash = hash;
    UUID next = first;
    boolean loopOne = true;
    while ((hash != origHash) || (loopOne))
    {
      Map.Entry<Integer, UUID> higher = this.ring.higherEntry(Integer.valueOf(hash));
      if (higher != null)
      {
        hash = ((Integer)higher.getKey()).intValue();
        UUID test = (UUID)higher.getValue();
        if (!first.equals(test))
        {
          next = test;
          break;
        }
      }
      else
      {
        Map.Entry<Integer, UUID> firstEntry = this.ring.firstEntry();
        hash = ((Integer)firstEntry.getKey()).intValue();
        UUID test = (UUID)firstEntry.getValue();
        if (!first.equals(test))
        {
          next = test;
          break;
        }
      }
      if (loopOne)
      {
        origHash = hash;
        hash = -1;
        loopOne = false;
      }
    }
    return next;
  }
  
  private Map.Entry<Integer, UUID> nextRingEntry(int hash)
  {
    Map.Entry<Integer, UUID> next = this.ring.higherEntry(Integer.valueOf(hash));
    if (next == null) {
      next = this.ring.firstEntry();
    }
    return next;
  }
  
  public List<UUID> getN(Object key, int n)
  {
    if (this.ring.isEmpty()) {
      return null;
    }
    List<UUID> nUUIDs = new ArrayList();
    UUID first = get(key);
    nUUIDs.add(first);
    for (int i = 1; i < n; i++)
    {
      int hash = hash(key);
      Map.Entry<Integer, UUID> next = nextRingEntry(hash);
      hash = ((Integer)next.getKey()).intValue();
      int firstHash = hash;
      do
      {
        UUID test = (UUID)next.getValue();
        if (!nUUIDs.contains(test))
        {
          nUUIDs.add(test);
          break;
        }
        next = nextRingEntry(hash);
        hash = ((Integer)next.getKey()).intValue();
      } while (hash != firstHash);
    }
    return nUUIDs;
  }
}
