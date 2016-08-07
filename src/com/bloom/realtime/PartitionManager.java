package com.bloom.runtime;

import com.bloom.cache.CacheAccessor;
import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.metaRepository.MDRepository;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.persistence.WactionStore;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.Server;
import com.bloom.security.WASecurityManager;
import com.bloom.uuid.UUID;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Endpoint;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.Member;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;

public class PartitionManager
  implements EntryListener<UUID, UUID>, IPartitionManager
{
  private static Logger logger = Logger.getLogger(PartitionManager.class);
  static ServerManager serverManager = null;
  private static String[] defaultGroups = { "default" };
  private static List<String> defaultList = Arrays.asList(defaultGroups);
  UUID cacheID;
  List<UUID> deploymentServers;
  public final ConsistentHashRing ring;
  final String cacheName;
  final UUID thisID;
  final MDRepository metadataRepository;
  final CacheAccessor cache;
  
  private static ServerManager serverManager()
  {
    if (serverManager == null) {
      serverManager = new ServerManager(null);
    }
    return serverManager;
  }
  
  public static void registerCache(String cacheName, UUID metaUUID)
  {
    serverManager().registerCache(cacheName, metaUUID);
  }
  
  public static void deregisterCache(MetaInfo.MetaObject obj)
  {
    serverManager().removeCacheForMetaObject(obj.uuid);
  }
  
  public static Set<String> getAllCacheNames()
  {
    return serverManager().getAllCacheNames();
  }
  
  public static void shutdown()
  {
    if (serverManager != null)
    {
      serverManager.doShutdown();
      serverManager = null;
    }
  }
  
  static class ResolverListener
    implements EntryListener<String, UUID>
  {
    List<String> seenCaches = new ArrayList();
    
    public void entryAdded(EntryEvent<String, UUID> event)
    {
      if (!this.seenCaches.contains(event.getKey())) {
        PartitionManager.serverManager.addReverseResolver((String)event.getKey(), (UUID)event.getValue());
      }
    }
    
    public void entryRemoved(EntryEvent<String, UUID> event)
    {
      if (this.seenCaches.contains(event.getKey())) {
        this.seenCaches.remove(event.getKey());
      }
    }
    
    public void entryUpdated(EntryEvent<String, UUID> event) {}
    
    public void entryEvicted(EntryEvent<String, UUID> event)
    {
      if (this.seenCaches.contains(event.getKey())) {
        this.seenCaches.remove(event.getKey());
      }
    }
    
    public void mapEvicted(MapEvent event) {}
    
    public void mapCleared(MapEvent event) {}
  }
  
  static class ServerManager
    implements EntryListener<UUID, MetaInfo.Server>
  {
    IMap<String, UUID> cacheResolver;
    IMap<String, List<UUID>> cacheReferences;
    IMap<String, Long> cacheStatus;
    UUID thisId;
    MDRepository metadataRepository = MetadataRepository.getINSTANCE();
    Map<UUID, List<String>> reverseResolver = new HashMap();
    final Set<UUID> serverGroups = new HashSet();
    final Map<String, PartitionManager> partitionManagers = new ConcurrentHashMap();
    
    private ServerManager()
    {
      this.thisId = HazelcastSingleton.getNodeId();
      this.cacheResolver = HazelcastSingleton.get().getMap("#cacheResolver");
      this.cacheReferences = HazelcastSingleton.get().getMap("#cacheReferences");
      this.cacheStatus = HazelcastSingleton.get().getMap("#cacheStatus");
      for (Map.Entry<String, UUID> entry : this.cacheResolver.entrySet()) {
        addReverseResolver((String)entry.getKey(), (UUID)entry.getValue());
      }
      this.cacheResolver.addEntryListener(new PartitionManager.ResolverListener(), true);
      this.metadataRepository.registerListenerForServer(this);
      try
      {
        updateServerGroups();
      }
      catch (MetaDataRepositoryException e)
      {
        PartitionManager.logger.error(e.getMessage());
      }
    }
    
    private void updateServerGroups()
      throws MetaDataRepositoryException
    {
      Set<UUID> valid;
      synchronized (this.serverGroups)
      {
        this.serverGroups.clear();
        valid = new HashSet();
        Set<Member> hzMembers = HazelcastSingleton.get().getCluster().getMembers();
        for (Member member : hzMembers) {
          valid.add(new UUID(member.getUuid()));
        }
        Set<MetaInfo.Server> servers = this.metadataRepository.getByEntityType(EntityType.SERVER, WASecurityManager.TOKEN);
        for (MetaInfo.Server server : servers) {
          if (valid.contains(server.getUuid())) {
            this.serverGroups.add(server.getUuid());
          }
        }
      }
      if (PartitionManager.logger.isInfoEnabled()) {
        PartitionManager.logger.info("ServerManager has " + this.serverGroups);
      }
    }
    
    public void registerCache(String cacheName, UUID metaUUID)
    {
      this.cacheResolver.put(cacheName, metaUUID);
    }
    
    public Set<String> getAllCacheNames()
    {
      return this.cacheResolver.keySet();
    }
    
    public void keepCacheReference(String cacheName)
    {
      try
      {
        this.cacheReferences.lock(cacheName);
        List<UUID> refs = (List)this.cacheReferences.get(cacheName);
        if (refs == null) {
          refs = new ArrayList();
        }
        refs.add(this.thisId);
        this.cacheReferences.put(cacheName, refs);
      }
      finally
      {
        this.cacheReferences.unlock(cacheName);
      }
    }
    
    private void addReverseResolver(String cacheName, UUID metaUUID)
    {
      synchronized (this.reverseResolver)
      {
        List<String> currentForUUID = (List)this.reverseResolver.get(metaUUID);
        if (currentForUUID == null) {
          currentForUUID = new ArrayList();
        }
        if (!currentForUUID.contains(cacheName)) {
          currentForUUID.add(cacheName);
        }
        this.reverseResolver.put(metaUUID, currentForUUID);
      }
      if (PartitionManager.logger.isInfoEnabled()) {
        PartitionManager.logger.info("ServerManager Register managed object " + cacheName + " for " + metaUUID);
      }
    }
    
    public UUID getRelatedMetaObject(String cacheName)
    {
      return (UUID)this.cacheResolver.get(cacheName);
    }
    
    public void removeCacheForMetaObject(UUID metaId)
    {
      synchronized (this.reverseResolver)
      {
        List<String> cacheNames = (List)this.reverseResolver.get(metaId);
        if (cacheNames == null) {
          return;
        }
        for (String cacheName : cacheNames)
        {
          PartitionManager pm = (PartitionManager)this.partitionManagers.get(cacheName);
          if (pm != null)
          {
            pm.cache.close();
            this.partitionManagers.remove(cacheName);
            WactionStore.remove(metaId);
            this.reverseResolver.remove(metaId);
            try
            {
              this.cacheReferences.lock(cacheName);
              List<UUID> refs = (List)this.cacheReferences.get(cacheName);
              if (refs != null)
              {
                if (refs.contains(this.thisId)) {
                  refs.remove(this.thisId);
                }
                Set<UUID> currentServers = this.serverGroups;
                Iterator<UUID> refIt = refs.iterator();
                while (refIt.hasNext())
                {
                  UUID ref = (UUID)refIt.next();
                  if (!currentServers.contains(ref)) {
                    refIt.remove();
                  }
                }
                if (refs.size() == 0)
                {
                  this.cacheReferences.remove(cacheName);
                  this.cacheResolver.remove(cacheName);
                  this.cacheStatus.remove(cacheName);
                }
                else
                {
                  this.cacheReferences.put(cacheName, refs);
                }
              }
            }
            finally
            {
              this.cacheReferences.unlock(cacheName);
            }
          }
        }
      }
    }
    
    /* Error */
    public List<UUID> getAllServers()
    {
      // Byte code:
      //   0: aload_0
      //   1: getfield 11	com/bloom/runtime/PartitionManager$ServerManager:serverGroups	Ljava/util/Set;
      //   4: dup
      //   5: astore_1
      //   6: monitorenter
      //   7: new 69	java/util/ArrayList
      //   10: dup
      //   11: aload_0
      //   12: getfield 11	com/bloom/runtime/PartitionManager$ServerManager:serverGroups	Ljava/util/Set;
      //   15: invokespecial 89	java/util/ArrayList:<init>	(Ljava/util/Collection;)V
      //   18: aload_1
      //   19: monitorexit
      //   20: areturn
      //   21: astore_2
      //   22: aload_1
      //   23: monitorexit
      //   24: aload_2
      //   25: athrow
      // Line number table:
      //   Java source line #236	-> byte code offset #0
      //   Java source line #237	-> byte code offset #7
      //   Java source line #238	-> byte code offset #21
      // Local variable table:
      //   start	length	slot	name	signature
      //   0	26	0	this	ServerManager
      //   5	18	1	Ljava/lang/Object;	Object
      //   21	4	2	localObject1	Object
      // Exception table:
      //   from	to	target	type
      //   7	20	21	finally
      //   21	24	21	finally
    }
    
    public void addManagedPartition(PartitionManager manager)
    {
      synchronized (this.partitionManagers)
      {
        this.partitionManagers.put(manager.cacheName, manager);
      }
      update(manager);
    }
    
    public void update(PartitionManager manager)
    {
      List<UUID> servers = manager.deploymentServers;
      manager.setServers(servers);
    }
    
    private void modifyDeploymentGroups()
    {
      try
      {
        updateServerGroups();
      }
      catch (MetaDataRepositoryException e)
      {
        PartitionManager.logger.error(e.getMessage());
      }
      synchronized (this.partitionManagers)
      {
        for (PartitionManager manager : this.partitionManagers.values()) {
          update(manager);
        }
      }
    }
    
    public void doShutdown()
    {
      for (IPartitionManager manager : this.partitionManagers.values()) {
        manager.doShutdown();
      }
    }
    
    public void entryAdded(EntryEvent<UUID, MetaInfo.Server> event)
    {
      modifyDeploymentGroups();
    }
    
    public void entryRemoved(EntryEvent<UUID, MetaInfo.Server> event)
    {
      modifyDeploymentGroups();
    }
    
    public void entryUpdated(EntryEvent<UUID, MetaInfo.Server> event)
    {
      modifyDeploymentGroups();
    }
    
    public void entryEvicted(EntryEvent<UUID, MetaInfo.Server> event) {}
    
    public void mapEvicted(MapEvent event) {}
    
    public void mapCleared(MapEvent event) {}
  }
  
  public PartitionManager(String cacheName, int numReplicas, CacheAccessor cache)
  {
    this.cacheName = cacheName;
    this.cache = cache;
    this.thisID = HazelcastSingleton.getNodeId();
    this.metadataRepository = MetadataRepository.getINSTANCE();
    this.cacheID = serverManager().getRelatedMetaObject(cacheName);
    this.ring = new ConsistentHashRing(cacheName, numReplicas);
    this.metadataRepository.registerListerForDeploymentInfo(this);
    try
    {
      setDeploymentServers();
    }
    catch (MetaDataRepositoryException e)
    {
      logger.error(e.getMessage());
    }
    if (logger.isInfoEnabled()) {
      logger.info("Create PartitionManager " + cacheName + (this.cacheID == null ? "standalone" : new StringBuilder().append(" for ").append(this.cacheID).toString()));
    }
    serverManager().addManagedPartition(this);
    serverManager().keepCacheReference(cacheName);
  }
  
  public void doShutdown()
  {
    this.ring.shutDown();
  }
  
  Object cacheLock = new Object();
  
  private void setDeploymentServers()
    throws MetaDataRepositoryException
  {
    synchronized (this.cacheLock)
    {
      if (this.cacheID != null)
      {
        Collection<UUID> wd = this.metadataRepository.getServersForDeployment(this.cacheID, WASecurityManager.TOKEN);
        
        Collection<UUID> whereDeployed = new ArrayList();
        for (Object obj : wd) {
          if ((obj instanceof UUID)) {
            whereDeployed.add((UUID)obj);
          } else if ((obj instanceof Collection)) {
            whereDeployed.addAll((Collection)obj);
          }
        }
        Map<UUID, Endpoint> map = HazelcastSingleton.getActiveMembersMap();
        this.deploymentServers = new ArrayList();
        for (UUID s : whereDeployed) {
          if (map.containsKey(s)) {
            this.deploymentServers.add(s);
          }
        }
        if (this.deploymentServers.isEmpty()) {
          serverManager().removeCacheForMetaObject(this.cacheID);
        }
      }
      else
      {
        this.deploymentServers = serverManager().getAllServers();
      }
      if (logger.isInfoEnabled()) {
        logger.info("Partition Manager " + this.cacheName + " in Servers {" + this.deploymentServers + "}");
      }
    }
  }
  
  public void addUpdateListener(ConsistentHashRing.UpdateListener listener)
  {
    this.ring.addUpdateListener(listener);
  }
  
  public int getNumberOfPartitions()
  {
    return this.ring.getNumberOfPartitions();
  }
  
  public void removeUpdateListener(ConsistentHashRing.UpdateListener listener)
  {
    this.ring.removeUpdateListener(listener);
  }
  
  public void setServers(List<UUID> servers)
  {
    if (logger.isInfoEnabled()) {
      logger.info("Partition Manager for " + this.cacheName + " in Servers {" + servers + "}");
    }
    this.ring.set(servers);
  }
  
  private boolean nullEqual(Object one, Object two)
  {
    if ((one == null) && (two == null)) {
      return true;
    }
    if ((one == null ? 1 : 0) != (two == null ? 1 : 0)) {
      return false;
    }
    return one.equals(two);
  }
  
  private void modifyDeploymentServers(UUID objid)
    throws MetaDataRepositoryException
  {
    if (this.cacheID == null) {
      this.cacheID = serverManager().getRelatedMetaObject(this.cacheName);
    }
    if ((this.cacheID == null) || (this.cacheID.equals(objid)))
    {
      setDeploymentServers();
      serverManager().update(this);
    }
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
  
  public void entryAdded(EntryEvent<UUID, UUID> event)
  {
    UUID objid = (UUID)event.getKey();
    try
    {
      modifyDeploymentServers(objid);
    }
    catch (MetaDataRepositoryException e)
    {
      logger.error(e.getMessage());
    }
  }
  
  public void entryRemoved(EntryEvent<UUID, UUID> event)
  {
    UUID serverID = (UUID)event.getValue();
    UUID objid = (UUID)event.getKey();
    try
    {
      modifyDeploymentServers(objid);
    }
    catch (MetaDataRepositoryException e)
    {
      logger.error(e.getMessage());
    }
    if (this.thisID.equals(serverID)) {
      serverManager().removeCacheForMetaObject(objid);
    }
  }
  
  public void entryUpdated(EntryEvent<UUID, UUID> event)
  {
    UUID objid = (UUID)event.getKey();
    try
    {
      modifyDeploymentServers(objid);
    }
    catch (MetaDataRepositoryException e)
    {
      logger.error(e.getMessage());
    }
  }
  
  public void entryEvicted(EntryEvent<UUID, UUID> event)
  {
    UUID serverID = (UUID)event.getValue();
    UUID objid = (UUID)event.getKey();
    try
    {
      modifyDeploymentServers(objid);
    }
    catch (MetaDataRepositoryException e)
    {
      logger.error(e.getMessage());
    }
    if (this.thisID.equals(serverID)) {
      serverManager().removeCacheForMetaObject(objid);
    }
  }
  
  public void mapEvicted(MapEvent event) {}
  
  public void mapCleared(MapEvent event) {}
}
