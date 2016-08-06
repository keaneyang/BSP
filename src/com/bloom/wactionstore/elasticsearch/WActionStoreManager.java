package com.bloom.wactionstore.elasticsearch;

import com.bloom.anno.EntryPoint;
import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.runtime.BaseServer;
import com.bloom.wactionstore.CheckpointManager;
import com.bloom.wactionstore.Utility;
import com.bloom.wactionstore.WAction;
import com.bloom.wactionstore.WActionStores;
import com.bloom.wactionstore.base.WActionStoreManagerBase;
import com.bloom.wactionstore.constants.Capability;
import com.bloom.wactionstore.constants.Constants;
import com.bloom.wactionstore.constants.NameType;
import com.bloom.wactionstore.exceptions.CapabilityException;
import com.bloom.wactionstore.exceptions.ConnectionException;
import com.bloom.wactionstore.exceptions.WActionStoreException;
import com.bloom.wactionstore.exceptions.WActionStoreMissingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Joiner;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.validation.constraints.NotNull;
import org.apache.log4j.Logger;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.hppc.ObjectLookupContainer;
import org.elasticsearch.common.hppc.cursors.ObjectCursor;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

public class WActionStoreManager
  extends WActionStoreManagerBase
{
  private static final Class<WActionStoreManager> thisClass = WActionStoreManager.class;
  private static final Logger logger = Logger.getLogger(thisClass);
  private static final Pattern SERVER_NAME_SEPARATOR = Pattern.compile("[,]");
  private static final Pattern SERVER_PORT_SEPARATOR = Pattern.compile("[:]");
  private static final Pattern REMOTE_SERVER_CHARACTERS = Pattern.compile("[.:]");
  private static final String REMOTE_SERVER_SUBSTITUTE_PATTERN = "_";
  private static final Locale WACTIONSTORE_LOCALE = Locale.getDefault();
  private static final Capability[] Capabilities = { Capability.SELECT, Capability.INSERT, Capability.DELETE, Capability.PROJECTIONS, Capability.FROM, Capability.WHERE, Capability.ORDER_BY, Capability.GROUP_BY, Capability.LIKE, Capability.REGEX, Capability.BETWEEN, Capability.IN_SET, Capability.AGGREGATION_MIN, Capability.AGGREGATION_MAX, Capability.AGGREGATION_COUNT, Capability.AGGREGATION_SUM, Capability.AGGREGATION_AVG, Capability.DATA_ID, Capability.DATA_TIMESTAMP, Capability.DATA_TIME_TO_LIVE, Capability.DATA_ANY };
  private static final long CONNECTION_RETRY_INTERVAL = 2000L;
  private static final String PROVIDER_NAME = "elasticsearch";
  private final String clusterName;
  private final String dataPath;
  private Node node = null;
  private Client client = null;
  
  @EntryPoint(usedBy=2)
  public WActionStoreManager(String providerName, Map<String, Object> properties, String instanceName)
  {
    super(providerName, instanceName, Capabilities);
    String externalEsClusterName = System.getProperty("com.bloom.config.external-es");
    if (externalEsClusterName != null) {
      this.clusterName = externalEsClusterName;
    } else {
      this.clusterName = getClusterName(properties);
    }
    this.dataPath = Utility.getPropertyString(properties, "elasticsearch.data_path");
  }
  
  private Client connectTransportClient(String serverIdentity)
  {
    String[] servers = SERVER_NAME_SEPARATOR.split(serverIdentity);
    ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();
    settingsBuilder.put("client.transport.sniff", true);
    if (this.dataPath != null) {
      settingsBuilder.put("path.data", this.dataPath);
    }
    String clusterName = null;
    TransportClient transportClient = null;
    for (String server : servers)
    {
      String[] tokens = SERVER_PORT_SEPARATOR.split(server);
      String serverName = tokens[0];
      String serverClusterName = tokens.length > 1 ? tokens[1] : clusterName;
      if ((clusterName == null) && (serverClusterName != null))
      {
        clusterName = serverClusterName;
        settingsBuilder.put("cluster.name", clusterName);
        transportClient = new TransportClient(settingsBuilder.build());
      }
      if (serverClusterName == null) {
        logger.error(String.format("Do not have a cluster name for server '%s'", new Object[] { serverName }));
      }
      if ((serverClusterName != null) && (!serverClusterName.equals(clusterName)))
      {
        logger.error(String.format("Cluster name '%s' for server '%s' does not match primary cluster name '%s'", new Object[] { serverClusterName, serverName, clusterName }));
        transportClient = null;
      }
      if (transportClient != null) {
        transportClient.addTransportAddresses(new TransportAddress[] { new InetSocketTransportAddress(serverName, 9300) });
      }
    }
    boolean isValid = (transportClient != null) && (!transportClient.connectedNodes().isEmpty());
    reportTransportConnectResult(serverIdentity, clusterName, isValid);
    return transportClient;
  }
  
  private static void reportTransportConnectResult(String serverIdentity, String clusterName, boolean isValid)
  {
    if (isValid) {
      logger.info(String.format("Connected to cluster '%s'", new Object[] { clusterName }));
    } else {
      logger.error(String.format("Cannot connect to '%s'", new Object[] { serverIdentity }));
    }
  }
  
  private static String getDefaultClusterName()
  {
    String result = null;
    String clusterName = System.getProperty("com.bloom.config.clusterName");
    if (clusterName != null)
    {
      Matcher matcher = REMOTE_SERVER_CHARACTERS.matcher(clusterName);
      result = matcher.replaceAll("_");
    }
    return result;
  }
  
  @NotNull
  private static String getClusterName(Map<String, Object> properties)
  {
    Object clusterNameObject = properties == null ? null : properties.get("elasticsearch.cluster_name");
    String clusterName = clusterNameObject == null ? null : clusterNameObject.toString();
    if ((clusterName == null) || (clusterName.isEmpty())) {
      clusterName = getDefaultClusterName();
    }
    if ((clusterName == null) || (clusterName.isEmpty())) {
      clusterName = "<Local>";
    }
    return clusterName;
  }
  
  @EntryPoint(usedBy=2)
  public static String getInstanceName(Map<String, Object> properties)
  {
    String clusterName = getClusterName(properties);
    return "elasticsearch$" + clusterName;
  }
  
  private static String getIndexStoreType(Object storeType, String wActionStoreName)
  {
    String result;
    if ((storeType == null) || ("disk".equals(storeType)) || ("any".equals(storeType)))
    {
      result = "default";
    }
    else
    {
      if ("memory".equals(storeType)) {
        result = "memory";
      } else {
        throw new WActionStoreException(String.format("Cannot create WActionStore '%s'. Property 'elasticsearch.storage_type' must be one of 'any', 'disk', or 'memory'", new Object[] { wActionStoreName }));
      }
    }
    return result;
  }
  
  private static boolean isValidWActionStoreName(String wActionStoreName)
  {
    return (wActionStoreName != null) && (!wActionStoreName.isEmpty());
  }
  
  private Client connectNodeClient(String nodeName, String clusterName)
  {
    String networkHost = HazelcastSingleton.getBindingInterface();
    String unicastHosts = getUnicastHosts();
    
    String httpEnabled = System.getProperty("com.bloom.config.es.http.enabled", "true");
    ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();
    settingsBuilder.put("network.host", networkHost).put("node.name", nodeName).put("discovery.zen.ping.multicast.enabled", false).put("discovery.zen.ping.unicast.hosts", unicastHosts).put("http.enabled", httpEnabled);
    if (this.dataPath != null) {
      settingsBuilder.put("path.data", this.dataPath);
    }
    logger.info(String.format("Cluster '%s' has nodes '%s'", new Object[] { clusterName, unicastHosts }));
    this.node = NodeBuilder.nodeBuilder().clusterName(clusterName).settings(settingsBuilder.build()).node().start();
    
    Client result = null;
    Client nodeClient = this.node.client();
    RefreshRequest request = new RefreshRequest(new String[0]);
    for (int attempts = 1; attempts <= 5; attempts++) {
      try
      {
        nodeClient.admin().indices().refresh(request).actionGet();
        if (logger.isInfoEnabled())
        {
          ClusterHealthRequest healthRequest = new ClusterHealthRequest(new String[0]);
          logger.info("ElasticSearch number of data nodes in the cluster: " + ((ClusterHealthResponse)nodeClient.admin().cluster().health(healthRequest).actionGet()).getNumberOfDataNodes());
        }
        result = nodeClient;
      }
      catch (ClusterBlockException exception)
      {
        if (!exception.retryable())
        {
          logger.error(String.format("Cannot connect to '%s'", new Object[] { clusterName }), exception);
          break;
        }
        try
        {
          Thread.sleep(2000L);
        }
        catch (InterruptedException ignored) {}
      }
    }
    boolean isValid = result != null;
    reportTransportConnectResult(clusterName, clusterName, isValid);
    return nodeClient;
  }
  
  private String getUnicastHosts()
  {
    String localHost = HazelcastSingleton.getBindingInterface();
    Collection<String> unicastHosts = new ArrayList(1);
    Collection<Member> members = getClusterMembers();
    unicastHosts.add(localHost);
    for (Member member : members) {
      addUnicastHost(unicastHosts, member);
    }
    return Joiner.on(",").join(unicastHosts);
  }
  
  private static void addUnicastHost(Collection<String> unicastHosts, Member member)
  {
    SocketAddress socketAddress = member.getSocketAddress();
    if ((socketAddress instanceof InetSocketAddress))
    {
      InetSocketAddress inetSocketAddress = (InetSocketAddress)socketAddress;
      String addressString = inetSocketAddress.getAddress().getHostAddress();
      if (!unicastHosts.contains(addressString)) {
        unicastHosts.add(addressString);
      }
    }
  }
  
  private static Collection<Member> getClusterMembers()
  {
    HazelcastInstance instance = HazelcastSingleton.get();
    Cluster cluster = instance.getCluster();
    Set<Member> members = cluster.getMembers();
    return members != null ? members : new ArrayList(0);
  }
  
  private Client connectLocalClient()
  {
    this.node = NodeBuilder.nodeBuilder().local(true).node().start();
    logger.info(String.format("Connected to cluster '%s'", new Object[] { "<Local>" }));
    return this.node.client();
  }
  
  public String translateName(NameType nameType, String name)
  {
    if (nameType == NameType.WACTIONSTORE) {
      return name.toLowerCase(WACTIONSTORE_LOCALE);
    }
    return name;
  }
  
  public String[] getNames()
  {
    String[] result = null;
    if (isConnected()) {
      result = ((ClusterStateResponse)this.client.admin().cluster().prepareState().execute().actionGet()).getState().getMetaData().concreteAllIndices();
    }
    return result;
  }
  
  public WActionStore get(String wActionStoreName, Map<String, Object> properties)
  {
    WActionStore result = null;
    if ((isValidWActionStoreName(wActionStoreName)) && (isConnected()))
    {
      String actualName = translateName(NameType.WACTIONSTORE, wActionStoreName);
      IndicesExistsRequest request = Requests.indicesExistsRequest(new String[] { actualName });
      try
      {
        IndicesExistsResponse response = (IndicesExistsResponse)this.client.admin().indices().exists(request).actionGet();
        if (response.isExists())
        {
          if ((properties != null) && (properties.containsKey("elasticsearch.time_to_live")))
          {
            properties.put("bloom_elasticsearch.time_to_live", properties.get("elasticsearch.time_to_live"));
            properties.remove("elasticsearch.time_to_live");
          }
          wActionStoreName = getActualNameWithoutTimeStamp(wActionStoreName);
          result = new WActionStore(this, wActionStoreName, properties, actualName);
        }
      }
      catch (NoNodeAvailableException exception)
      {
        throw new ConnectionException(getProviderName(), getInstanceName(), exception);
      }
    }
    return result;
  }
  
  private boolean checkIfIndexExists(String wActionStoreName)
  {
    String actualName = translateName(NameType.WACTIONSTORE, wActionStoreName);
    IndicesExistsRequest request = Requests.indicesExistsRequest(new String[] { actualName });
    try
    {
      IndicesExistsResponse response = (IndicesExistsResponse)this.client.admin().indices().exists(request).actionGet();
      if (response.isExists()) {
        return true;
      }
    }
    catch (NoNodeAvailableException exception)
    {
      throw new ConnectionException(getProviderName(), getInstanceName(), exception);
    }
    return false;
  }
  
  public String getActualNameWithoutTimeStamp(String actualName)
  {
    if (Constants.IsEsRollingIndexEnabled.equalsIgnoreCase("true"))
    {
      String[] splitByHash = actualName.split("%");
      String actualNameWithoutTimeStamp = splitByHash[0];
      return actualNameWithoutTimeStamp;
    }
    return actualName;
  }
  
  public WActionStore getUsingAlias(String wActionStoreName, Map<String, Object> properties)
  {
    if (Constants.IsEsRollingIndexEnabled.equalsIgnoreCase("true"))
    {
      if ((properties != null) && (!properties.containsKey("elasticsearch.time_to_live"))) {
        return get(wActionStoreName, properties);
      }
      IndexOperationsManager indexManager = getIndexManager();
      if ((isValidWActionStoreName(wActionStoreName)) && (isConnected()))
      {
        String actualName = translateName(NameType.WACTIONSTORE, wActionStoreName);
        String aliasName = "search_alias_" + actualName;
        ImmutableOpenMap<String, ImmutableOpenMap<String, AliasMetaData>> map_alias = indexManager.getAliasMapLocally();
        if ((map_alias == null) || (map_alias.isEmpty()))
        {
          map_alias = indexManager.getAliasMapFromES();
          indexManager.populateIndexCreationTimeMap(map_alias);
        }
        if (!map_alias.containsKey(aliasName))
        {
          if ((checkIfIndexExists(actualName)) && 
            (properties != null) && (properties.containsKey("elasticsearch.time_to_live")))
          {
            properties.put("bloom_elasticsearch.time_to_live", properties.get("elasticsearch.time_to_live"));
            properties.remove("elasticsearch.time_to_live");
            indexManager.addAliasToIndex(actualName, "search_alias_" + actualName);
            map_alias = indexManager.getAliasMapFromES();
            indexManager.populateIndexCreationTimeMap(map_alias);
          }
          return get(wActionStoreName, properties);
        }
        List<String> index_list = indexManager.getIndicesFromAliasName(aliasName, map_alias);
        String mostRecentIndex = indexManager.getMostRecentIndex(index_list);
        return get(mostRecentIndex, properties);
      }
    }
    else
    {
      return get(wActionStoreName, properties);
    }
    return null;
  }
  
  public boolean remove(String wActionStoreName)
  {
    boolean result = false;
    if ((isValidWActionStoreName(wActionStoreName)) && (isConnected())) {
      try
      {
        String actualName = translateName(NameType.WACTIONSTORE, wActionStoreName);
        logger.info("Deleting index for wactionstore " + wActionStoreName + " with index name " + actualName);
        DeleteIndexRequest request = Requests.deleteIndexRequest(actualName);
        DeleteIndexResponse response = (DeleteIndexResponse)this.client.admin().indices().delete(request).actionGet();
        result = (response != null) && (response.isAcknowledged());
        if (!result) {
          logger.warn("Failed to Delete index for wactionstore " + wActionStoreName + " with index name " + actualName);
        }
      }
      catch (IndexMissingException ignored)
      {
        logger.warn(String.format("WActionStore '%s' does not exist", new Object[] { wActionStoreName }));
      }
    }
    if (result)
    {
      if (Constants.IsEsRollingIndexEnabled.equalsIgnoreCase("true"))
      {
        IndexOperationsManager indexManager = getIndexManager();
        indexManager.getCreationTimeOfIndicesMap().remove(translateName(NameType.WACTIONSTORE, wActionStoreName));
        indexManager.updateLocalAliasMap();
      }
      logger.info("Successfully Deleted index for wactionstore " + wActionStoreName);
      CheckpointManager checkpointManager = getCheckpointManager();
      checkpointManager.remove(wActionStoreName);
      logger.info("Successfully Deleted checkpoint index for wactionstore " + wActionStoreName);
    }
    return result;
  }
  
  public boolean removeUsingAlias(String wActionStoreName)
  {
    if (Constants.IsEsRollingIndexEnabled.equalsIgnoreCase("true"))
    {
      boolean result = true;
      if ((isValidWActionStoreName(wActionStoreName)) && (isConnected()))
      {
        IndexOperationsManager indexManager = getIndexManager();
        String actualName = translateName(NameType.WACTIONSTORE, wActionStoreName);
        String aliasName = "search_alias_" + actualName;
        indexManager.updateLocalAliasMap();
        if (!indexManager.getAliasMapLocally().containsKey(aliasName))
        {
          WActionStores.terminateBackgroundTasks(wActionStoreName);
          return remove(wActionStoreName);
        }
        Iterator<ObjectCursor<String>> itr_alias = indexManager.getAliasMapLocally().keys().iterator();
        while (itr_alias.hasNext())
        {
          String alias_target = (String)((ObjectCursor)itr_alias.next()).value;
          if (alias_target.equalsIgnoreCase(aliasName))
          {
            ImmutableOpenMap<String, AliasMetaData> map_index = (ImmutableOpenMap)indexManager.getAliasMapLocally().get(alias_target);
            Iterator<ObjectCursor<String>> itr_index = map_index.keys().iterator();
            while (itr_index.hasNext()) {
              result = (remove((String)((ObjectCursor)itr_index.next()).value)) && (result);
            }
            WActionStores.terminateBackgroundTasks(wActionStoreName);
          }
        }
        return result;
      }
    }
    else
    {
      return remove(wActionStoreName);
    }
    return false;
  }
  
  public WActionStore create(String wActionStoreName, Map<String, Object> properties)
  {
    WActionStore result = null;
    if ((isValidWActionStoreName(wActionStoreName)) && (isConnected())) {
      try
      {
        int indices_count = 1;
        String actualName = translateName(NameType.WACTIONSTORE, wActionStoreName);
        String actualNameWithoutTimeStamp = getActualNameWithoutTimeStamp(actualName);
        ImmutableSettings.Builder indexSettings = ImmutableSettings.settingsBuilder();
        if ((properties != null) && (properties.containsKey("elasticsearch.shards")))
        {
          Integer numberOfShards = Utility.getPropertyInteger(properties, "elasticsearch.shards");
          if ((numberOfShards == null) || (numberOfShards.intValue() <= 0)) {
            throw new WActionStoreException(String.format("Cannot create WActionStore '%s'. Property 'elasticsearch.shards' must be greater than zero", new Object[] { wActionStoreName }));
          }
          indexSettings.put("number_of_shards", numberOfShards.intValue());
        }
        if ((properties != null) && (properties.containsKey("elasticsearch.replicas")))
        {
          Integer numberOfReplicas = Utility.getPropertyInteger(properties, "elasticsearch.replicas");
          if ((numberOfReplicas == null) || (numberOfReplicas.intValue() < 0)) {
            throw new WActionStoreException(String.format("Cannot create WActionStore '%s'. Property 'elasticsearch.replicas' must be greater than or equal to zero", new Object[] { wActionStoreName }));
          }
          indexSettings.put("number_of_replicas", numberOfReplicas.intValue());
        }
        if ((properties != null) && (properties.containsKey("elasticsearch.refresh_interval")))
        {
          String refreshInterval = Utility.getPropertyString(properties, "elasticsearch.refresh_interval");
          indexSettings.put("index.refresh_interval", refreshInterval);
        }
        if ((properties != null) && (properties.containsKey("elasticsearch.flush_interval")))
        {
          String flushInterval = Utility.getPropertyString(properties, "elasticsearch.flush_interval");
          indexSettings.put("index.translog.flush_threshold_ops", flushInterval);
        }
        if ((properties != null) && (properties.containsKey("elasticsearch.flush_threshold_period")))
        {
          String flushInterval = Utility.getPropertyString(properties, "elasticsearch.flush_threshold_period");
          indexSettings.put("index.translog.flush_threshold_period", flushInterval);
        }
        if ((properties != null) && (properties.containsKey("elasticsearch.flush_size")))
        {
          String flushSize = Utility.getPropertyString(properties, "elasticsearch.flush_size");
          indexSettings.put("index.translog.flush_threshold_size", flushSize);
        }
        if ((properties != null) && (properties.containsKey("elasticsearch.compress")))
        {
          String doCompress = Utility.getPropertyString(properties, "elasticsearch.compress");
          indexSettings.put("index.store.compress", doCompress);
        }
        if ((properties != null) && (properties.containsKey("elasticsearch.merge_num_threads")))
        {
          String numThreads = Utility.getPropertyString(properties, "elasticsearch.merge_num_threads");
          indexSettings.put("index.merge.scheduler.max_thread_count", numThreads);
        }
        if ((properties != null) && (properties.containsKey("elasticsearch.merge_type")))
        {
          String policy = Utility.getPropertyString(properties, "elasticsearch.merge_type");
          indexSettings.put("index.merge.policy.type", policy);
        }
        if ((properties != null) && (properties.containsKey("elasticsearch.merge_factor")))
        {
          String mergeFactor = Utility.getPropertyString(properties, "elasticsearch.merge_factor");
          indexSettings.put("index.merge.policy.merge_factor", mergeFactor);
        }
        String storeType = Utility.getPropertyString(properties, "elasticsearch.storage_type");
        String indexStoreType = getIndexStoreType(storeType, wActionStoreName);
        indexSettings.put("index.store.type", indexStoreType);
        
        CreateIndexRequest request = Requests.createIndexRequest(actualName).settings(indexSettings.build());
        CreateIndexResponse response = (CreateIndexResponse)this.client.admin().indices().create(request).actionGet();
        if ((properties != null) && (properties.containsKey("elasticsearch.time_to_live")) && (Constants.IsEsRollingIndexEnabled.equalsIgnoreCase("true")))
        {
          IndexOperationsManager indexManager = getIndexManager();
          properties.put("bloom_elasticsearch.time_to_live", properties.get("elasticsearch.time_to_live"));
          properties.remove("elasticsearch.time_to_live");
          indexManager.addAliasToIndex(actualName, "search_alias_" + actualNameWithoutTimeStamp);
          indexManager.getCreationTimeOfIndicesMap().put(actualName, Long.valueOf(indexManager.getCreationTimeOfIndexFromES(actualName)));
          indexManager.updateLocalAliasMap();
          indices_count = indexManager.getIndicesFromAliasName("search_alias_" + actualNameWithoutTimeStamp, indexManager.getAliasMapLocally()).size();
        }
        if ((response != null) && (response.isAcknowledged()) && (indices_count == 1))
        {
          wActionStoreName = getActualNameWithoutTimeStamp(wActionStoreName);
          result = new WActionStore(this, wActionStoreName, properties, actualName);
        }
      }
      catch (IndexAlreadyExistsException ignored)
      {
        logger.info(String.format("WActionStore '%s' already exists", new Object[] { wActionStoreName }));
      }
    }
    return result;
  }
  
  public com.bloom.wactionstore.WActionQuery prepareQuery(JsonNode queryJson)
    throws WActionStoreException
  {
    validateQuery(queryJson);
    
    String wActionStoreName = queryJson.path("from").get(0).asText();
    WActionStore wActionStore = getUsingAlias(wActionStoreName, null);
    if (wActionStore == null) {
      throw new WActionStoreMissingException(getProviderName(), getInstanceName(), wActionStoreName);
    }
    WActionQuery result;
    try
    {
      result = new WActionQuery(queryJson, new WActionStore[] { wActionStore });
    }
    catch (CapabilityException exception)
    {
      throw new CapabilityException(getProviderName(), exception.capabilityViolated);
    }
    return result;
  }
  
  public long delete(JsonNode queryJson)
    throws WActionStoreException
  {
    validateDelete(queryJson);
    
    String wActionStoreName = queryJson.path("from").get(0).asText();
    WActionStore wActionStore = get(wActionStoreName, null);
    if (wActionStore == null) {
      throw new WActionStoreMissingException(getProviderName(), getInstanceName(), wActionStoreName);
    }
    WActionDeleteByQuery statement = new WActionDeleteByQuery(queryJson, new WActionStore[] { wActionStore });
    WAction statementResult = (WAction)statement.execute().iterator().next();
    return statementResult.get("Result").asLong();
  }
  
  public void flush()
  {
    if (isConnected())
    {
      RefreshRequest request = new RefreshRequest(new String[0]);
      this.client.admin().indices().refresh(request).actionGet();
    }
  }
  
  public void fsync()
  {
    if (isConnected())
    {
      FlushRequest request = new FlushRequest(new String[0]);
      try
      {
        this.client.admin().indices().flush(request).actionGet();
      }
      catch (NoNodeAvailableException ignored) {}
    }
  }
  
  public void close()
  {
    if (this.client != null)
    {
      fsync();
      this.client.close();
      this.client = null;
    }
  }
  
  public void shutdown()
  {
    if (this.client != null)
    {
      fsync();
      shutdownLocalNode();
      this.client.close();
      this.client = null;
    }
  }
  
  private void shutdownLocalNode()
  {
    if (this.node != null)
    {
      logger.info(String.format("Shutting down local node in cluster '%s'", new Object[] { this.clusterName }));
      this.node.close();
      this.node = null;
    }
  }
  
  public synchronized Client getClient()
  {
    if (this.client == null) {
      connect();
    }
    return this.client;
  }
  
  private void connect()
  {
    if ("<Local>".equals(this.clusterName))
    {
      logger.info("Connecting local client");
      this.client = connectLocalClient();
    }
    else if (REMOTE_SERVER_CHARACTERS.matcher(this.clusterName).find())
    {
      logger.info(String.format("Connecting transport client to '%s'", new Object[] { this.clusterName }));
      TransportClient transportClient = (TransportClient)connectTransportClient(this.clusterName);
      if (transportClient != null)
      {
        ImmutableList<DiscoveryNode> nodes = transportClient.connectedNodes();
        if (!nodes.isEmpty()) {
          this.client = transportClient;
        }
      }
    }
    else
    {
      String nodeName = BaseServer.getServerName();
      logger.info(String.format("Connecting node '%s' to cluster '%s'", new Object[] { nodeName, this.clusterName }));
      this.client = connectNodeClient(nodeName, this.clusterName);
    }
  }
  
  private boolean isConnected()
  {
    return getClient() != null;
  }
  
  public SearchRequestBuilder prepareSearch(WActionStore wActionStore, JsonNode queryJson)
  {
    String actualName = wActionStore.getActualName();
    SearchType searchType = getSearchType(queryJson);
    SearchRequestBuilder result = null;
    if (isConnected())
    {
      IndexOperationsManager indexManager = getIndexManager();
      indexManager.updateLocalAliasMap();
      if (!indexManager.getAliasMapLocally().containsKey("search_alias_" + actualName)) {
        result = this.client.prepareSearch(new String[] { actualName }).setSearchType(searchType);
      } else {
        result = this.client.prepareSearch(new String[] { "search_alias_" + actualName }).setSearchType(searchType);
      }
    }
    return result;
  }
  
  private SearchType getSearchType(JsonNode queryJson)
  {
    boolean hasOrderBy = queryJson.get("orderby") != null;
    boolean hasGroupBy = queryJson.get("groupby") != null;
    return (hasOrderBy) || (hasGroupBy) ? SearchType.QUERY_THEN_FETCH : SearchType.QUERY_AND_FETCH;
  }
}
