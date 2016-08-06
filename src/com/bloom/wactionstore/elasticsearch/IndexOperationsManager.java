package com.bloom.wactionstore.elasticsearch;

import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.wactionstore.base.WActionStoreManagerBase;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.log4j.Logger;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.hppc.ObjectLookupContainer;
import org.elasticsearch.common.hppc.cursors.ObjectCursor;

public class IndexOperationsManager
{
  private ImmutableOpenMap<String, ImmutableOpenMap<String, AliasMetaData>> map_alias = null;
  private ConcurrentMap creation_time_of_indices_map = new ConcurrentHashMap();
  private final WActionStoreManager manager;
  private static final Class<DataType> thisClass = DataType.class;
  private static final Logger logger = Logger.getLogger(thisClass);
  private static final String ROLLINGINDEXLOCK = "RollingIndexLock";
  
  public IndexOperationsManager(WActionStoreManagerBase manager)
  {
    this.manager = ((WActionStoreManager)manager);
  }
  
  public long getCreationTimeOfIndexLocally(String indexName)
  {
    return ((Long)getCreationTimeOfIndicesMap().get(indexName)).longValue();
  }
  
  public ConcurrentMap<String, Long> getCreationTimeOfIndicesMap()
  {
    return this.creation_time_of_indices_map;
  }
  
  public synchronized void updateLocalAliasMap()
  {
    this.map_alias = getAliasMapFromES();
  }
  
  public ImmutableOpenMap<String, ImmutableOpenMap<String, AliasMetaData>> getAliasMapLocally()
  {
    return this.map_alias;
  }
  
  public void addAliasToIndex(String indexName, String aliasName)
  {
    this.manager.getClient().admin().indices().prepareAliases().addAlias(indexName, aliasName).execute().actionGet();
  }
  
  public void removeAliasFromIndex(String indexName, String aliasName)
  {
    this.manager.getClient().admin().indices().prepareAliases().removeAlias(indexName, aliasName).execute().actionGet();
  }
  
  public long getDbExpireTime(Map<String, Object> properties)
  {
    String TTL = (String)properties.get("bloom_elasticsearch.time_to_live");
    String[] part = TTL.split("(?<=\\d)(?=\\D)");
    long time = Integer.parseInt(part[0]);
    if (part.length > 1)
    {
      String count = part[1];
      switch (count)
      {
      case "ms": 
        time = time;
        break;
      case "s": 
        time *= 1000L;
        break;
      case "m": 
        time = time * 1000L * 60L;
        break;
      case "h": 
        time = time * 1000L * 60L * 60L;
        break;
      case "d": 
        time = time * 1000L * 60L * 60L * 24L;
        break;
      case "w": 
        time = time * 1000L * 60L * 60L * 24L * 7L;
      }
    }
    return time;
  }
  
  public boolean checkIfTTLForIndexExpired(String indexName, Map<String, Object> properties)
  {
    long created_time = getCreationTimeOfIndexLocally(indexName);
    long now = System.currentTimeMillis();
    long diff = now - created_time;
    if (diff * 2L >= getDbExpireTime(properties)) {
      return true;
    }
    return false;
  }
  
  public String getMostOldIndex(List<String> index_list)
  {
    if (index_list.size() == 1) {
      return (String)index_list.get(0);
    }
    int j = 0;
    for (int i = 1; i < index_list.size(); i++) {
      if (getCreationTimeOfIndexLocally((String)index_list.get(i)) - getCreationTimeOfIndexLocally((String)index_list.get(j)) < 0L) {
        j = i;
      }
    }
    return (String)index_list.get(j);
  }
  
  public String getMostRecentIndex(List<String> index_list)
  {
    if (index_list.size() == 1) {
      return (String)index_list.get(0);
    }
    int j = 0;
    for (int i = 1; i < index_list.size(); i++) {
      if (getCreationTimeOfIndexLocally((String)index_list.get(i)) - getCreationTimeOfIndexLocally((String)index_list.get(j)) > 0L) {
        j = i;
      }
    }
    return (String)index_list.get(j);
  }
  
  public synchronized void populateIndexCreationTimeMap(ImmutableOpenMap<String, ImmutableOpenMap<String, AliasMetaData>> map_alias)
  {
    this.creation_time_of_indices_map.clear();
    Iterator<ObjectCursor<String>> itr_alias = map_alias.keys().iterator();
    while (itr_alias.hasNext())
    {
      String alias = (String)((ObjectCursor)itr_alias.next()).value;
      ImmutableOpenMap<String, AliasMetaData> map_index = (ImmutableOpenMap)map_alias.get(alias);
      Iterator<ObjectCursor<String>> itr_index = map_index.keys().iterator();
      while (itr_index.hasNext())
      {
        String indexName = (String)((ObjectCursor)itr_index.next()).value;
        long creation_time_of_index = getCreationTimeOfIndexFromES(indexName);
        this.creation_time_of_indices_map.putIfAbsent(indexName, Long.valueOf(creation_time_of_index));
      }
    }
  }
  
  public long getCreationTimeOfIndexFromES(String indexName)
  {
    return ((ClusterStateResponse)this.manager.getClient().admin().cluster().prepareState().execute().actionGet()).getState().getMetaData().index(indexName).getCreationDate();
  }
  
  public List<String> getIndicesFromAliasName(String aliasName, ImmutableOpenMap<String, ImmutableOpenMap<String, AliasMetaData>> map_alias)
  {
    List index_list = new ArrayList();
    Iterator<ObjectCursor<String>> itr_alias = map_alias.keys().iterator();
    while (itr_alias.hasNext())
    {
      String alias_target = (String)((ObjectCursor)itr_alias.next()).value;
      if (alias_target.equalsIgnoreCase(aliasName))
      {
        ImmutableOpenMap<String, AliasMetaData> map_index = (ImmutableOpenMap)map_alias.get(alias_target);
        Iterator<ObjectCursor<String>> itr_index = map_index.keys().iterator();
        while (itr_index.hasNext()) {
          index_list.add(((ObjectCursor)itr_index.next()).value);
        }
      }
    }
    return index_list;
  }
  
  public ImmutableOpenMap<String, ImmutableOpenMap<String, AliasMetaData>> getAliasMapFromES()
  {
    return ((ClusterStateResponse)this.manager.getClient().admin().cluster().prepareState().execute().actionGet()).getState().getMetaData().getAliases();
  }
  
  public void checkForRollingIndex(WActionStore wActionStore)
  {
    if (wActionStore.getProperties().containsKey("bloom_elasticsearch.time_to_live"))
    {
      WActionStoreManager manager = (WActionStoreManager)wActionStore.getManager();
      String actualName = wActionStore.getActualName();
      ILock rollingIndexLock = null;
      String indexName = wActionStore.getIndexName();
      if (checkIfTTLForIndexExpired(indexName, wActionStore.getProperties())) {
        try
        {
          rollingIndexLock = HazelcastSingleton.get().getLock("RollingIndexLock");
          rollingIndexLock.lock();
          
          updateLocalAliasMap();
          ImmutableOpenMap<String, ImmutableOpenMap<String, AliasMetaData>> map_alias = getAliasMapLocally();
          populateIndexCreationTimeMap(map_alias);
          List<String> index_list_search = getIndicesFromAliasName("search_alias_" + actualName, map_alias);
          indexName = getMostRecentIndex(index_list_search);
          if (checkIfTTLForIndexExpired(indexName, wActionStore.getProperties()))
          {
            boolean orphanIndexExists = checkForAnyOrphanIndexesAndClean(actualName, manager);
            if (orphanIndexExists) {
              logger.warn("There was an orphan index which just got cleaned up as part of cleaning process - White Blood Cell code of Waction Store");
            }
            String oldIndexName = getMostOldIndex(index_list_search);
            performRollingIndex(wActionStore, oldIndexName, map_alias);
          }
        }
        finally
        {
          if (rollingIndexLock != null) {
            rollingIndexLock.unlock();
          }
        }
      }
    }
  }
  
  private void performRollingIndex(WActionStore wActionStore, String oldIndexName, ImmutableOpenMap<String, ImmutableOpenMap<String, AliasMetaData>> map_alias)
  {
    WActionStoreManager manager = (WActionStoreManager)wActionStore.getManager();
    String actualName = wActionStore.getActualName();
    int indices_count = getIndicesFromAliasName("search_alias_" + actualName, map_alias).size();
    long now = System.currentTimeMillis();
    String newIndexName = actualName + "%" + Long.toString(now);
    wActionStore.setIndexName(newIndexName);
    Map<String, Object> properties = wActionStore.getProperties();
    if ((properties != null) && (properties.containsKey("bloom_elasticsearch.time_to_live")))
    {
      properties.put("elasticsearch.time_to_live", properties.get("bloom_elasticsearch.time_to_live"));
      properties.remove("bloom_elasticsearch.time_to_live");
    }
    if (indices_count > 2)
    {
      removeAliasFromIndex(oldIndexName, "search_alias_" + actualName);
      manager.remove(oldIndexName);
    }
    manager.create(newIndexName, properties);
    wActionStore.setDataType(wActionStore.getDataTypeName(), wActionStore.getDataTypeSchema(), null);
  }
  
  private boolean checkForAnyOrphanIndexesAndClean(String actualName, WActionStoreManager manager)
  {
    String[] indices = manager.getNames();
    List<String> index_list = new ArrayList(Arrays.asList(indices));
    ImmutableOpenMap<String, ImmutableOpenMap<String, AliasMetaData>> map_alias = getAliasMapLocally();
    Iterator<ObjectCursor<String>> itr_alias = map_alias.keys().iterator();
    while (itr_alias.hasNext())
    {
      String alias = (String)((ObjectCursor)itr_alias.next()).value;
      if (alias.startsWith("search_alias_"))
      {
        ImmutableOpenMap<String, AliasMetaData> map_index = (ImmutableOpenMap)map_alias.get(alias);
        Iterator<ObjectCursor<String>> itr_index = map_index.keys().iterator();
        while (itr_index.hasNext())
        {
          String key = (String)((ObjectCursor)itr_index.next()).value;
          if (!index_list.contains(key)) {
            logger.error("This should not happen, index list and alias map should be in sync wrt indices");
          } else {
            index_list.remove(key);
          }
        }
      }
    }
    for (int i = 0; i < index_list.size(); i++)
    {
      String indexName = manager.getActualNameWithoutTimeStamp((String)index_list.get(i));
      if (indexName.equalsIgnoreCase(actualName)) {
        return manager.remove((String)index_list.get(i));
      }
    }
    return false;
  }
}
