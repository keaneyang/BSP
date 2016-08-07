package com.bloom.runtime.monitor;

import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.classloading.WALoader;
import com.bloom.health.AppHealth;
import com.bloom.health.HealthMonitor;
import com.bloom.health.HealthMonitorImpl;
import com.bloom.health.HealthRecord;
import com.bloom.health.HealthRecordBuilder;
import com.bloom.health.HealthRecordCollection;
import com.bloom.intf.PersistenceLayer;
import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.metaRepository.MDClientOps;
import com.bloom.metaRepository.MDRepository;
import com.bloom.metaRepository.MetaDataDBDetails;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.persistence.PersistenceFactory;
import com.bloom.persistence.PersistenceFactory.PersistingPurpose;
import com.bloom.proc.BaseProcess;
import com.bloom.runtime.Pair;
import com.bloom.runtime.Server;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.components.Subscriber;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfoStatus;
import com.bloom.runtime.meta.MetaInfo.Flow;
import com.bloom.runtime.meta.MetaInfo.Initializer;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.StatusInfo;
import com.bloom.runtime.meta.MetaInfo.StatusInfo.Status;
import com.bloom.security.WASecurityManager;
import com.bloom.uuid.AuthToken;
import com.bloom.uuid.UUID;
import com.bloom.waction.WactionKey;
import com.bloom.wactionstore.DataType;
import com.bloom.wactionstore.InternalType;
import com.bloom.wactionstore.Utility;
import com.bloom.wactionstore.WAction;
import com.bloom.wactionstore.WActionQuery;
import com.bloom.wactionstore.WActionStore;
import com.bloom.wactionstore.WActionStoreManager;
import com.bloom.wactionstore.WActionStores;
import com.bloom.wactionstore.exceptions.WActionStoreMissingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hazelcast.core.Client;
import com.hazelcast.core.ClientListener;
import com.hazelcast.core.ClientService;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.bloom.event.Event;
import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.ITaskEvent;
import com.bloom.runtime.containers.WAEvent;

import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.log4j.Logger;
import org.eclipse.persistence.exceptions.PersistenceUnitLoadingException;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.ShardSearchFailure;

@PropertyTemplate(name="MonitorModel", type=AdapterType.internal, properties={@com.bloom.anno.PropertyTemplateProperty(name="derbyDbDir", type=String.class, required=false, defaultValue="derby/monitorDb")}, outputType=MonitorBatchEvent.class)
public class MonitorModel
  extends BaseProcess
  implements Subscriber, Runnable
{
  public static final boolean persistInES = true;
  private static Logger logger;
  private static final UUID MonitorModelID;
  public static final UUID KAFKA_SERVER_UUID;
  public static final UUID KAFKA_ENTITY_UUID;
  public static final UUID ES_SERVER_UUID;
  public static final UUID ES_ENTITY_UUID;
  public static final int NANOS = 1000000000;
  public static final int PERCENT = 100;
  private static int DEFAULT_MAX_EVENTS;
  private static int DEFAULT_MAX_EVENTS_FOR_LOAD;
  private static int DEFAULT_DB_EXPIRE_TIME;
  private static int persistPeriod;
  private static long lastPersistTime;
  private static long HEALTH_WRITE_PERIOD;
  private static long KAFKA_COLLECTION_PERIOD;
  private static long ES_COLLECTION_PERIOD;
  private static volatile MonitorModel instance;
  private static Deque<List<MonitorEvent>> insertQueue;
  
  public static UUID getMonitorModelID()
  {
    return MonitorModelID;
  }
  
  private final ScheduledExecutorService executor = Server.getServer().getScheduler();
  private static final KafkaMonitor kafkaMonitor;
  private static final ElasticsearchMonitor elasticsearchMonitor;
  static boolean running;
  static boolean persistMonitor;
  static boolean persistHealth;
  static Lock persistChangeLock;
  private static WActionStoreManager monitorModelPersistedWActionStore;
  private static DataType monitorEventDataType;
  private static WActionStoreManager healthRecordWActionStore;
  private static DataType healthRecordDataType;
  private MDRepository mdRepository = MetadataRepository.getINSTANCE();
  static List<String> rateDatum;
  
  public static MonitorModel getOrCreateInstance()
  {
    if (instance == null) {
      synchronized (MonitorModel.class)
      {
        if (instance == null) {
          instance = new MonitorModel();
        }
      }
    }
    return instance;
  }
  
  public static MonitorModel getInstance()
  {
    return instance;
  }
  
  public static void shutdown()
    throws Exception
  {
    if (instance != null)
    {
      instance.close();
      instance = null;
    }
  }
  
  static
  {
    logger = Logger.getLogger(MonitorModel.class);
    MonitorModelID = new UUID("9d31a65a-169e-6f32-b105-cea16c56c149");
    
    KAFKA_SERVER_UUID = new UUID("5420C7DB-2292-4535-99E5-A376C5F50042");
    KAFKA_ENTITY_UUID = new UUID("561457DF-9992-AF11-BBE5-E472C0F0BC4F");
    ES_SERVER_UUID = new UUID("3441C7DB-2292-4525-09EE-3656C5F50042");
    ES_ENTITY_UUID = new UUID("F61457DF-9992-AF98-B0E5-E472C0F0BC40");
    
    DEFAULT_MAX_EVENTS = 1000;
    DEFAULT_MAX_EVENTS_FOR_LOAD = 10000;
    DEFAULT_DB_EXPIRE_TIME = 86400000;
    persistPeriod = 60000;
    lastPersistTime = 0L;
    HEALTH_WRITE_PERIOD = 30000L;
    KAFKA_COLLECTION_PERIOD = 10000L;
    ES_COLLECTION_PERIOD = 10000L;
    
    insertQueue = new ArrayDeque();
    
    kafkaMonitor = new KafkaMonitor();
    elasticsearchMonitor = new ElasticsearchMonitor();
    running = true;
    persistMonitor = true;
    persistHealth = true;
    persistChangeLock = new ReentrantLock();
    
    monitorModelPersistedWActionStore = null;
    monitorEventDataType = null;
    
    healthRecordWActionStore = null;
    healthRecordDataType = null;
    
    rateDatum = new ArrayList();
    
    rateDatum.add(MonitorEvent.Type.LOOKUPS_RATE.toString());
    rateDatum.add(MonitorEvent.Type.INPUT_RATE.toString());
    rateDatum.add(MonitorEvent.Type.RECEIVED_RATE.toString());
    rateDatum.add(MonitorEvent.Type.LOCAL_HITS_RATE.toString());
    rateDatum.add(MonitorEvent.Type.REMOTE_HITS_RATE.toString());
  }
  
  static Map<MonitorData.Key, MonitorData> monitorDataMap = new HashMap();
  
  public static String getWhereClause(Map<String, Object> params)
  {
    if (params.isEmpty()) {
      return "";
    }
    StringBuilder result = new StringBuilder();
    result.append("WHERE ");
    Iterator<String> entries = params.keySet().iterator();
    while (entries.hasNext())
    {
      String entry = (String)entries.next();
      if (entry.equalsIgnoreCase("serverID")) {
        result.append("se.serverID = :serverID ");
      }
      if (entry.equalsIgnoreCase("entityID")) {
        result.append("se.entityID = :entityID ");
      }
      if (entry.equalsIgnoreCase("startTime")) {
        result.append("se.timeStamp >= :startTime ");
      }
      if (entry.equalsIgnoreCase("datumNames")) {
        result.append("se.type in :datumNames ");
      }
      if (entry.equalsIgnoreCase("endTime")) {
        result.append("se.timeStamp <= :endTime ");
      }
      if (entries.hasNext()) {
        result.append("AND ");
      }
    }
    return result.toString();
  }
  
  public void setPersistence(boolean turnOn)
  {
    synchronized (persistChangeLock)
    {
      persistMonitor = (turnOn) && (Server.persistenceIsEnabled());
      initPersistence();
    }
  }
  
  private static Map<MonitorData.Key, Map<String, Object>> getCurrentValues(Set<MonitorData.Key> keysToGet)
  {
    Map<MonitorData.Key, Map<String, Object>> result = new HashMap();
    for (MonitorData.Key key : keysToGet)
    {
      MonitorData md = (MonitorData)monitorDataMap.get(key);
      if (md != null) {
        result.put(key, md.getValues());
      }
    }
    return result;
  }
  
  private static void addCurrentMonitorValues(Map<MonitorData.Key, MonitorData> values)
  {
    for (Map.Entry<MonitorData.Key, MonitorData> entry : values.entrySet())
    {
      MonitorData currData = (MonitorData)monitorDataMap.get(entry.getKey());
      if (currData == null) {
        currData = new MonitorData(((MonitorData.Key)entry.getKey()).entityID, ((MonitorData.Key)entry.getKey()).serverID);
      }
      currData.addLatest((MonitorData)entry.getValue());
      monitorDataMap.put(entry.getKey(), currData);
    }
  }
  
  private MonitorModel()
  {
    setPersistence(monitorPersistenceIsEnabled());
    WALoader loader = WALoader.get();
    try
    {
      loader.loadClass("com.bloom.runtime.monitor.MonitorBatchEvent");
    }
    catch (ClassNotFoundException e)
    {
      logger.error("Could not load class", e);
    }
    loadCacheFromDb();
    
    HazelcastSingleton.get().getMap("MonitorModelToServerMap").put("MonitorModel", Server.getServer().getServerID());
    
    HazelcastSingleton.get().getCluster().addMembershipListener(new MembershipListener()
    {
      public void memberRemoved(MembershipEvent arg0)
      {
        UUID objectID = new UUID(arg0.getMember().getUuid());
        MonitorCollector.reportStateChange(objectID, MetaInfo.StatusInfo.serializeToString(arg0.getMember().getUuid(), arg0.getMember().getSocketAddress().getHostName().toString(), "SERVER", "PRESENT", "ABSENT"));
      }
      
      public void memberAttributeChanged(MemberAttributeEvent arg0)
      {
        UUID objectID = new UUID(arg0.getMember().getUuid());
        MonitorCollector.reportStateChange(objectID, MetaInfo.StatusInfo.serializeToString(arg0.getMember().getUuid(), arg0.getMember().getSocketAddress().getHostName().toString(), "SERVER", "NOT KNOWN", arg0.getValue().toString()));
      }
      
      public void memberAdded(MembershipEvent arg0)
      {
        UUID objectID = new UUID(arg0.getMember().getUuid());
        MonitorCollector.reportStateChange(objectID, MetaInfo.StatusInfo.serializeToString(arg0.getMember().getUuid(), arg0.getMember().getSocketAddress().getHostName().toString(), "SERVER", "ABSENT", "PRESENT"));
      }
    });
    HazelcastSingleton.get().getClientService().addClientListener(new ClientListener()
    {
      public void clientConnected(Client arg0)
      {
        try
        {
          UUID objectID = new UUID(arg0.getUuid());
          MetaInfo.Server removedAgent = (MetaInfo.Server)MonitorModel.this.mdRepository.getMetaObjectByUUID(objectID, WASecurityManager.TOKEN);
          if ((removedAgent != null) && (removedAgent.isAgent)) {
            MonitorCollector.reportStateChange(objectID, MetaInfo.StatusInfo.serializeToString(arg0.getUuid(), arg0.getSocketAddress().toString(), "AGENT", "ABSENT", "PRESENT"));
          }
        }
        catch (MetaDataRepositoryException e)
        {
          MonitorModel.logger.warn(e.getMessage(), e);
        }
      }
      
      public void clientDisconnected(Client arg0)
      {
        try
        {
          UUID objectID = new UUID(arg0.getUuid());
          MetaInfo.Server removedAgent = (MetaInfo.Server)MonitorModel.this.mdRepository.getMetaObjectByUUID(objectID, WASecurityManager.TOKEN);
          if ((removedAgent != null) && (removedAgent.isAgent)) {
            MonitorCollector.reportStateChange(objectID, MetaInfo.StatusInfo.serializeToString(arg0.getUuid(), arg0.getSocketAddress().toString(), "AGENT", "PRESENT", "ABSENT"));
          }
        }
        catch (MetaDataRepositoryException e)
        {
          MonitorModel.logger.warn(e.getMessage(), e);
        }
      }
    });
    this.executor.scheduleWithFixedDelay(kafkaMonitor, KAFKA_COLLECTION_PERIOD, KAFKA_COLLECTION_PERIOD, TimeUnit.MILLISECONDS);
    
    this.executor.scheduleWithFixedDelay(elasticsearchMonitor, ES_COLLECTION_PERIOD, ES_COLLECTION_PERIOD, TimeUnit.MILLISECONDS);
  }
  
  private void initPersistence()
  {
    synchronized (persistChangeLock)
    {
      if (persistMonitor)
      {
        logger.info("PersistMonitor is true");
        Map<String, Object> properties = new HashMap();
        properties.put("elasticsearch.time_to_live", Integer.toString(getDbExpireTime()));
        properties.put("elasticsearch.refresh_interval", "30");
        properties.put("elasticsearch.flush_interval", "50000");
        properties.put("elasticsearch.flush_size", "1gb");
        properties.put("elasticsearch.flush_threshold_period", "5h");
        properties.put("elasticsearch.compress", "false");
        properties.put("elasticsearch.merge_num_threads", "1");
        properties.put("elasticsearch.merge_type", "log_byte_size");
        properties.put("elasticsearch.merge_factor", "30");
        if (monitorModelPersistedWActionStore == null)
        {
          monitorModelPersistedWActionStore = WActionStores.getInstance(properties);
          monitorEventDataType = InternalType.MONITORING.getDataType(monitorModelPersistedWActionStore, properties);
        }
      }
      if (persistHealth)
      {
        logger.info("Persist health is true");
        Map<String, Object> properties = createPropertiesForHealthES();
        initHealthStore(properties);
      }
    }
  }
  
  public static Map<String, Object> createPropertiesForHealthES()
  {
    Map<String, Object> properties = new HashMap();
    properties.put("elasticsearch.time_to_live", Integer.toString(getDbExpireTime()));
    properties.put("elasticsearch.refresh_interval", "30");
    properties.put("elasticsearch.flush_interval", "50000");
    properties.put("elasticsearch.flush_size", "1gb");
    properties.put("elasticsearch.flush_threshold_period", "5h");
    properties.put("elasticsearch.compress", "false");
    properties.put("elasticsearch.merge_num_threads", "1");
    properties.put("elasticsearch.merge_type", "log_byte_size");
    properties.put("elasticsearch.merge_factor", "30");
    return properties;
  }
  
  public static void initHealthStore(Map<String, Object> properties)
  {
    if ((healthRecordWActionStore == null) || (healthRecordDataType == null))
    {
      healthRecordWActionStore = WActionStores.getInstance(properties);
      healthRecordDataType = InternalType.HEALTH.getDataType(healthRecordWActionStore, properties);
    }
  }
  
  public static DataType getHealthRecordDataType()
  {
    if (healthRecordDataType == null)
    {
      Map<String, Object> properties = createPropertiesForHealthES();
      initHealthStore(properties);
    }
    return healthRecordDataType;
  }
  
  public static WActionStoreManager getHealthRecordDataStore()
  {
    if (healthRecordWActionStore == null)
    {
      Map<String, Object> properties = createPropertiesForHealthES();
      initHealthStore(properties);
    }
    return healthRecordWActionStore;
  }
  
  private void loadCacheFromDb()
  {
    long startTime = System.currentTimeMillis() - MonitorData.HISTORY_EXPIRE_TIME;
    List<MonitorEvent> olderEvents = getDbMonitorEvents(null, null, Long.valueOf(startTime), Long.valueOf(System.currentTimeMillis()), null, "ASC", DEFAULT_MAX_EVENTS_FOR_LOAD);
    logger.info("loading " + olderEvents.size() + " number of events from cache");
    writeToCache(olderEvents);
  }
  
  private static Map<Pair<String, UUID>, Map<UUID, String>> getAllAppDetails()
    throws MetaDataRepositoryException
  {
    Map<Pair<String, UUID>, Map<UUID, String>> allAppUUIDs = new HashMap();
    Set<MetaInfo.MetaObject> allApps = MetadataRepository.getINSTANCE().getByEntityType(EntityType.APPLICATION, WASecurityManager.TOKEN);
    for (MetaInfo.MetaObject obj : allApps)
    {
      MetaInfo.Flow app = (MetaInfo.Flow)obj;
      Map<UUID, String> appUUIDs = null;
      Map<EntityType, LinkedHashSet<UUID>> appE = app.getObjects();
      if (!appE.isEmpty())
      {
        Pair<String, UUID> key = Pair.make(app.nsName + "." + app.name, app.uuid);
        appUUIDs = new LinkedHashMap();
        for (Map.Entry<EntityType, LinkedHashSet<UUID>> appList : appE.entrySet())
        {
          for (UUID uuid : (LinkedHashSet)appList.getValue())
          {
            MetaInfo.MetaObject subObj = MetadataRepository.getINSTANCE().getMetaObjectByUUID(uuid, WASecurityManager.TOKEN);
            if (subObj != null) {
              appUUIDs.put(uuid, subObj.nsName + "." + subObj.name);
            }
          }
          if (((EntityType)appList.getKey()).equals(EntityType.FLOW)) {
            for (UUID flowUUID : (LinkedHashSet)appList.getValue())
            {
              MetaInfo.Flow flow = (MetaInfo.Flow)MetadataRepository.getINSTANCE().getMetaObjectByUUID(flowUUID, WASecurityManager.TOKEN);
              if (flow != null)
              {
                Map<EntityType, LinkedHashSet<UUID>> flowE = flow.getObjects();
                for (Map.Entry<EntityType, LinkedHashSet<UUID>> flowList : flowE.entrySet()) {
                  for (UUID uuid : (LinkedHashSet)flowList.getValue())
                  {
                    MetaInfo.MetaObject subObj = MetadataRepository.getINSTANCE().getMetaObjectByUUID(uuid, WASecurityManager.TOKEN);
                    if (subObj != null) {
                      appUUIDs.put(uuid, subObj.nsName + "." + subObj.name);
                    }
                  }
                }
              }
            }
          }
        }
        allAppUUIDs.put(key, appUUIDs);
      }
    }
    return allAppUUIDs;
  }
  
  private static UUID getContainingAppID(Map<Pair<String, UUID>, Map<UUID, String>> allApps, UUID entityID)
  {
    for (Map.Entry<Pair<String, UUID>, Map<UUID, String>> entry : allApps.entrySet()) {
      if (((Map)entry.getValue()).containsKey(entityID)) {
        return (UUID)((Pair)entry.getKey()).second;
      }
    }
    return null;
  }
  
  private static String getEntityName(Map<Pair<String, UUID>, Map<UUID, String>> allApps, UUID appID, UUID entityID)
  {
    for (Map.Entry<Pair<String, UUID>, Map<UUID, String>> entry : allApps.entrySet()) {
      if (((UUID)((Pair)entry.getKey()).second).equals(appID)) {
        return (String)((Map)entry.getValue()).get(entityID);
      }
    }
    return null;
  }
  
  private static List<MonitorEvent> rollupComponentData(Map<UUID, MetaInfo.Server> allServers, Map<Pair<String, UUID>, Map<UUID, String>> allApps, List<MonitorEvent> rawEvents)
  {
    List<MonitorEvent> componentRollupEvents = new ArrayList();
    if (rawEvents.isEmpty()) {
      return componentRollupEvents;
    }
    Set<MonitorData.Key> keysToGet = new HashSet();
    MonitorEvent monEvent;
    for (Iterator i$ = rawEvents.iterator(); i$.hasNext();)
    {
      monEvent = (MonitorEvent)i$.next();
      for (UUID serverID : allServers.keySet()) {
        if (!monEvent.serverID.equals(serverID)) {
          keysToGet.add(new MonitorData.Key(monEvent.entityID, serverID));
        }
      }
    }
    
    Map<MonitorData.Key, Map<String, Object>> currentData = null;
    Map<UUID, Set<UUID>> entityServerMap = null;
    if (!keysToGet.isEmpty())
    {
      currentData = getCurrentValues(keysToGet);
      if (!currentData.isEmpty())
      {
        entityServerMap = new HashMap();
        for (MonitorData.Key key : currentData.keySet())
        {
          Set<UUID> serverSet = (Set)entityServerMap.get(key.entityID);
          if (serverSet == null)
          {
            serverSet = new HashSet();
            entityServerMap.put(key.entityID, serverSet);
          }
          serverSet.add(key.serverID);
        }
      }
    }
    Long ts = null;
    for (MonitorEvent monEvent : rawEvents) {
      if ((monEvent.type != MonitorEvent.Type.STATUS_CHANGE) && 
        (monEvent.type != MonitorEvent.Type.KAFKA_BROKERS) && 
        (!monEvent.entityID.equals(monEvent.serverID)))
      {
        if (ts == null) {
          ts = Long.valueOf(monEvent.timeStamp);
        }
        MonitorEvent rollupEvent = new MonitorEvent(MonitorEvent.RollupUUID, monEvent.entityID, monEvent.type, Long.valueOf(0L), Long.valueOf(monEvent.timeStamp));
        rollupEvent.valueLong = null;
        List<Pair<String, String>> stringRollups = null;
        Long longRollups = null;
        MetaInfo.Server monEventServer = (MetaInfo.Server)allServers.get(monEvent.serverID);
        if (monEventServer != null)
        {
          if (monEvent.valueString != null)
          {
            stringRollups = new ArrayList();
            
            stringRollups.add(Pair.make(monEventServer.name, monEvent.valueString));
          }
          else
          {
            longRollups = monEvent.valueLong;
          }
          if (entityServerMap != null)
          {
            Set<UUID> serversForEntity = (Set)entityServerMap.get(monEvent.entityID);
            if (serversForEntity != null) {
              for (UUID serverID : serversForEntity) {
                if ((!MonitorEvent.RollupUUID.equals(serverID)) && 
                  (!monEvent.serverID.equals(serverID)))
                {
                  MonitorData.Key key = new MonitorData.Key(monEvent.entityID, serverID);
                  Map<String, Object> serverCurrentMap = (Map)currentData.get(key);
                  if (serverCurrentMap != null)
                  {
                    MetaInfo.Server otherEventServer = (MetaInfo.Server)allServers.get(serverID);
                    Object value = serverCurrentMap.get(monEvent.type.name());
                    if ((value != null) && (otherEventServer != null)) {
                      if ((value instanceof String)) {
                        stringRollups.add(Pair.make(otherEventServer.name, (String)value));
                      } else if ((value instanceof Long)) {
                        longRollups = Long.valueOf(longRollups.longValue() + ((Long)value).longValue());
                      }
                    }
                  }
                }
              }
            }
          }
          if (stringRollups != null)
          {
            Collections.sort(stringRollups, new Comparator()
            {
              public int compare(Pair<String, String> o1, Pair<String, String> o2)
              {
                int c1 = ((String)o1.first).compareTo((String)o2.first);
                if (c1 != 0) {
                  return c1;
                }
                return ((String)o1.second).compareTo((String)o2.second);
              }
            });
            String stringVal = null;
            for (Pair<String, String> pair : stringRollups)
            {
              String pairVal = (String)pair.first + ":" + (String)pair.second;
              stringVal = stringVal + "," + pairVal;
            }
            rollupEvent.valueString = stringVal;
          }
          else if (longRollups != null)
          {
            rollupEvent.valueLong = longRollups;
          }
          componentRollupEvents.add(rollupEvent);
        }
      }
    }
    return componentRollupEvents;
  }
  
  private static List<MonitorEvent> createAppRollupEvents(Map<UUID, MetaInfo.Server> allServers, Map<Pair<String, UUID>, Map<UUID, String>> allApps, List<MonitorEvent> rawEvents)
  {
    List<MonitorEvent> appRollupEvents = new ArrayList();
    if (rawEvents.isEmpty()) {
      return appRollupEvents;
    }
    Map<UUID, String> entityThreads = new HashMap();
    UUID serverID = null;
    Long ts = null;
    for (MonitorEvent monEvent : rawEvents)
    {
      if (serverID == null) {
        serverID = monEvent.serverID;
      }
      if (ts == null) {
        ts = Long.valueOf(monEvent.timeStamp);
      }
      if (monEvent.type.equals(MonitorEvent.Type.CPU_THREAD)) {
        if (!MonitorEvent.RollupUUID.equals(serverID))
        {
          MetaInfo.Server s = (MetaInfo.Server)allServers.get(serverID);
          if (s != null) {
            entityThreads.put(monEvent.entityID, s.name + ":" + monEvent.valueString);
          }
        }
        else
        {
          entityThreads.put(monEvent.entityID, monEvent.valueString);
        }
      }
    }
    Set<String> seenThreads = new HashSet();
    Map<UUID, Map<MonitorEvent.Type, Object>> currentAppValues = new HashMap();
    for (Pair<String, UUID> pair : allApps.keySet())
    {
      Map<MonitorEvent.Type, Object> currentAppData = new HashMap();
      currentAppValues.put(pair.second, currentAppData);
    }
    for (MonitorEvent monEvent : rawEvents)
    {
      UUID containingAppID = getContainingAppID(allApps, monEvent.entityID);
      if (containingAppID != null)
      {
        String entityName = getEntityName(allApps, containingAppID, monEvent.entityID);
        if (entityName != null)
        {
          if (monEvent.type.equals(MonitorEvent.Type.CPU_RATE))
          {
            String thread = (String)entityThreads.get(monEvent.entityID);
            if (thread != null)
            {
              String[] threads = null;
              if (MonitorEvent.RollupUUID.equals(serverID))
              {
                threads = thread.split("[,]");
              }
              else
              {
                threads = new String[1];
                threads[0] = thread;
              }
              boolean seen = false;
              for (String aThread : threads) {
                if (seenThreads.contains(aThread))
                {
                  seen = true;
                  break;
                }
              }
              if (seen) {
                continue;
              }
              seenThreads.add(thread);
            }
          }
          Map<MonitorEvent.Type, Object> currentAppData = (Map)currentAppValues.get(containingAppID);
          if (currentAppData == null)
          {
            currentAppData = new HashMap();
            currentAppValues.put(containingAppID, currentAppData);
          }
          Object currValue = currentAppData.get(monEvent.type);
          try
          {
            if (monEvent.valueString != null)
            {
              if (currValue == null) {
                currValue = monEvent.valueString;
              } else if (!((String)currValue).contains(monEvent.valueString)) {
                currValue = currValue + "," + monEvent.valueString;
              }
            }
            else if (monEvent.type.equals(MonitorEvent.Type.LATEST_ACTIVITY)) {
              currValue = Long.valueOf(currValue == null ? monEvent.valueLong.longValue() : Math.max(((Long)currValue).longValue(), monEvent.valueLong.longValue()));
            } else {
              currValue = Long.valueOf(currValue == null ? monEvent.valueLong.longValue() : ((Long)currValue).longValue() + monEvent.valueLong.longValue());
            }
          }
          catch (Throwable t)
          {
            logger.error("Problem obtaining current value from " + monEvent, t);
          }
          currentAppData.put(monEvent.type, currValue);
        }
      }
    }
    UUID appId;
    for (Map.Entry<UUID, Map<MonitorEvent.Type, Object>> appEntry : currentAppValues.entrySet())
    {
      appId = (UUID)appEntry.getKey();
      for (Map.Entry<MonitorEvent.Type, Object> eventEntry : ((Map)appEntry.getValue()).entrySet())
      {
        MonitorEvent appEvent = new MonitorEvent(serverID, appId, (MonitorEvent.Type)eventEntry.getKey(), Long.valueOf(0L), ts);
        Object value = eventEntry.getValue();
        appEvent.valueLong = ((value instanceof Long) ? (Long)value : null);
        appEvent.valueString = ((value instanceof String) ? (String)value : null);
        appRollupEvents.add(appEvent);
      }
    }
   
    return appRollupEvents;
  }
  
  static Map<UUID, Long> serverTxMap = new HashMap();
  static Map<UUID, Long> serverRxMap = new HashMap();
  
  private static List<MonitorEvent> createElasticsearchRollupEvents(List<MonitorEvent> rawEvents)
  {
    List<MonitorEvent> serverRollupEvents = new ArrayList();
    if (rawEvents.isEmpty()) {
      return serverRollupEvents;
    }
    Long txTimeStamp = null;
    Long rxTimeStamp = null;
    for (MonitorEvent e : rawEvents) {
      if (e.type == MonitorEvent.Type.ES_TX_BYTES)
      {
        serverTxMap.put(e.serverID, e.valueLong);
        txTimeStamp = Long.valueOf(e.timeStamp);
      }
      else if (e.type == MonitorEvent.Type.ES_RX_BYTES)
      {
        serverRxMap.put(e.serverID, e.valueLong);
        rxTimeStamp = Long.valueOf(e.timeStamp);
      }
    }
    if (txTimeStamp != null)
    {
      long value = 0L;
      Long a;
      for (Iterator i$ = serverTxMap.values().iterator(); i$.hasNext(); value += a.longValue()) {
        a = (Long)i$.next();
      }
      serverRollupEvents.add(new MonitorEvent(ES_SERVER_UUID, ES_ENTITY_UUID, MonitorEvent.Type.ES_TX_BYTES, Long.valueOf(value), txTimeStamp));
    }
    if (rxTimeStamp != null)
    {
      long value = 0L;
      Long a;
      for (Iterator i$ = serverRxMap.values().iterator(); i$.hasNext(); value += a.longValue()) {
        a = (Long)i$.next();
      }
      serverRollupEvents.add(new MonitorEvent(ES_SERVER_UUID, ES_ENTITY_UUID, MonitorEvent.Type.ES_RX_BYTES, Long.valueOf(value), txTimeStamp));
    }
    return serverRollupEvents;
  }
  
  private static List<MonitorEvent> createServerRollupEvents(List<MonitorEvent> rawEvents)
  {
    List<MonitorEvent> serverRollupEvents = new ArrayList();
    if (rawEvents.isEmpty()) {
      return serverRollupEvents;
    }
    UUID serverID = null;
    Long ts = null;
    Map<MonitorEvent.Type, Object> currentServerData = new HashMap();
    for (MonitorEvent monEvent : rawEvents)
    {
      if (serverID == null) {
        serverID = monEvent.serverID;
      }
      if (ts == null) {
        ts = Long.valueOf(monEvent.timeStamp);
      }
      if ((monEvent.type != MonitorEvent.Type.STATUS_CHANGE) && 
      
        (monEvent.type != MonitorEvent.Type.CPU_RATE))
      {
        Object currValue = currentServerData.get(monEvent.type);
        try
        {
          if (monEvent.valueString != null)
          {
            if (currValue == null) {
              currValue = monEvent.valueString;
            } else if (!((String)currValue).contains(monEvent.valueString)) {
              currValue = currValue + "," + monEvent.valueString;
            }
          }
          else if (monEvent.type.equals(MonitorEvent.Type.LATEST_ACTIVITY)) {
            currValue = Long.valueOf(currValue == null ? monEvent.valueLong.longValue() : Math.max(((Long)currValue).longValue(), monEvent.valueLong.longValue()));
          } else {
            currValue = Long.valueOf(currValue == null ? monEvent.valueLong.longValue() : ((Long)currValue).longValue() + monEvent.valueLong.longValue());
          }
        }
        catch (Throwable t)
        {
          logger.error("Problem obtaining current value from " + monEvent, t);
        }
        currentServerData.put(monEvent.type, currValue);
      }
    }
    for (Map.Entry<MonitorEvent.Type, Object> eventEntry : currentServerData.entrySet())
    {
      MonitorEvent appEvent = new MonitorEvent(serverID, serverID, (MonitorEvent.Type)eventEntry.getKey(), Long.valueOf(0L), ts);
      Object value = eventEntry.getValue();
      appEvent.valueLong = ((value instanceof Long) ? (Long)value : null);
      appEvent.valueString = ((value instanceof String) ? (String)value : null);
      serverRollupEvents.add(appEvent);
    }
    return serverRollupEvents;
  }
  
  private static List<MonitorEvent> augmentWithRollupEvents(List<MonitorEvent> events)
    throws MetaDataRepositoryException
  {
    Map<Pair<String, UUID>, Map<UUID, String>> allApps = getAllAppDetails();
    Set<MetaInfo.Server> servers = MetadataRepository.getINSTANCE().getByEntityType(EntityType.SERVER, WASecurityManager.TOKEN);
    Map<UUID, MetaInfo.Server> allServers = new HashMap();
    for (MetaInfo.Server server : servers) {
      allServers.put(server.uuid, server);
    }
    List<MonitorEvent> allRollupEvents = new ArrayList();
    List<MonitorEvent> componentRollupEvents = rollupComponentData(allServers, allApps, events);
    List<MonitorEvent> appByServerRollupEvents = createAppRollupEvents(allServers, allApps, events);
    List<MonitorEvent> appRollupEvents = createAppRollupEvents(allServers, allApps, componentRollupEvents);
    List<MonitorEvent> serverRollupEvents = createServerRollupEvents(events);
    List<MonitorEvent> elasticsearchRollupEvents = createElasticsearchRollupEvents(events);
    
    allRollupEvents.addAll(events);
    allRollupEvents.addAll(componentRollupEvents);
    allRollupEvents.addAll(appByServerRollupEvents);
    allRollupEvents.addAll(appRollupEvents);
    allRollupEvents.addAll(serverRollupEvents);
    allRollupEvents.addAll(elasticsearchRollupEvents);
    
    return allRollupEvents;
  }
  
  public static MonitorBatchEvent processBatch(MonitorBatchEvent monEventBatch)
    throws Exception
  {
    if (getOrCreateInstance() == null) {
      return null;
    }
    if (monEventBatch.events == null) {
      return null;
    }
    List<MonitorEvent> removethese = new ArrayList();
    for (MonitorEvent e : monEventBatch.events) {
      if (e.type == MonitorEvent.Type.KAFKA_BROKERS)
      {
        kafkaMonitor.addKafkaBrokers(e.valueString);
        removethese.add(e);
      }
    }
    monEventBatch.events.removeAll(removethese);
    
    List<MonitorEvent> events = monEventBatch.events;
    events = augmentWithRollupEvents(events);
    
    MonitorBatchEvent batchEvent = new MonitorBatchEvent(System.currentTimeMillis(), events);
    writeToCache(events);
    synchronized (persistChangeLock)
    {
      if (persistMonitor) {
        persistInES(events);
      }
    }
    instance.updateHealthReport(events);
    return batchEvent;
  }
  
  private long currentHealthCreateTime = 0L;
  private HealthRecordBuilder currentHealthReportBuilder = new HealthRecordBuilder();
  private MetaDataDBDetails metaDataDBDetails = new MetaDataDBDetails();
  
  private void updateHealthReport(List<MonitorEvent> monEvents)
    throws Exception
  {
    long now = System.currentTimeMillis();
    if (this.currentHealthCreateTime == 0L)
    {
      this.currentHealthCreateTime = now;
      List<MonitorEvent> gapEvents = getMonitorEventsSinceLastHealthReport(this.currentHealthCreateTime);
      if (gapEvents != null) {
        updateHealthReport(gapEvents);
      }
    }
    if (now > this.currentHealthCreateTime + HEALTH_WRITE_PERIOD) {
      rollHealthReport(now);
    }
    for (MonitorEvent monEvent : monEvents) {
      try
      {
        MetaInfo.MetaObject mo = MetadataRepository.getINSTANCE().getMetaObjectByUUID(monEvent.entityID, WASecurityManager.TOKEN);
        if (monEvent.type == MonitorEvent.Type.LOG_ERROR)
        {
          String fullName = mo == null ? monEvent.getIDString() : mo.getFullName();
          String typeName = mo == null ? "" : mo.type.name();
          this.currentHealthReportBuilder.addIssue(fullName, typeName, monEvent.valueString);
        }
        else if (monEvent.type == MonitorEvent.Type.STATUS_CHANGE)
        {
          String fullName = mo == null ? monEvent.getIDString() : mo.getFullName();
          String typeName = mo == null ? "" : mo.type.name();
          this.currentHealthReportBuilder.addStateChange(fullName, typeName, monEvent.timeStamp, monEvent.valueString);
        }
        else if (mo != null)
        {
          this.currentHealthReportBuilder.addValue(monEvent.serverID.toString(), mo.getFullName(), mo.type, monEvent.type, monEvent.timeStamp, monEvent.valueString, monEvent.valueLong);
        }
        else if (logger.isDebugEnabled())
        {
          logger.debug("Could not find meta object for " + monEvent.serverID + " " + monEvent.entityID);
        }
      }
      catch (MetaDataRepositoryException e)
      {
        logger.warn("Error trying to look up meta object", e);
      }
      catch (Exception e)
      {
        logger.warn("Error processing event " + monEvent, e);
      }
    }
  }
  
  public void receive(Object linkID, ITaskEvent event)
    throws Exception
  {
    MonitorBatchEvent monEventBatch = (MonitorBatchEvent)((WAEvent)event.batch().first()).data;
    processBatch(monEventBatch);
  }
  
  public void receiveImpl(int channel, Event event)
    throws Exception
  {
    MonitorBatchEvent monEventBatch = (MonitorBatchEvent)event;
    processBatch(monEventBatch);
  }
  
  public static class ResultKey
    extends WactionKey
  {
    private static final long serialVersionUID = 2995663006723860813L;
    private String stringRep;
    
    public ResultKey() {}
    
    public ResultKey(UUID id, UUID key)
    {
      this.id = id;
      this.key = key;
      this.stringRep = toString();
    }
    
    public void setId(String id)
    {
      this.id = new UUID(id);
      this.stringRep = toString();
    }
    
    public String getId()
    {
      return this.id.toString();
    }
    
    public void setKey(String id)
    {
      this.key = new UUID(id);
      this.stringRep = toString();
    }
    
    public String getKey()
    {
      return this.key.toString();
    }
    
    public int hashCode()
    {
      return this.stringRep.hashCode();
    }
    
    public boolean equals(Object obj)
    {
      if ((obj instanceof ResultKey)) {
        return this.stringRep.equals(((ResultKey)obj).stringRep);
      }
      return false;
    }
    
    public String toString()
    {
      return "{\"id\":\"" + this.id + "\",\"key\":\"" + this.key + "\"}";
    }
  }
  
  private Map<MonitorEvent.Type, List<Pair<Long, Object>>> getMonitorEventsAsMap(UUID compId, UUID serverId, MonitorData historyData, List<String> datumNames, Long startTime, Long endTime)
  {
    List<MonitorEvent> monEvList = getMonitorEvents(compId, serverId, historyData, datumNames, startTime, endTime);
    Map<MonitorEvent.Type, List<Pair<Long, Object>>> typedEvents = new HashMap();
    if ((monEvList == null) || (monEvList.isEmpty())) {
      return typedEvents;
    }
    for (MonitorEvent monEvent : monEvList)
    {
      List<Pair<Long, Object>> typedEventList = (List)typedEvents.get(monEvent.type);
      if (typedEventList == null)
      {
        typedEventList = new ArrayList();
        typedEvents.put(monEvent.type, typedEventList);
      }
      typedEventList.add(Pair.make(Long.valueOf(monEvent.timeStamp), monEvent.valueString != null ? monEvent.valueString : monEvent.valueLong));
    }
    for (Map.Entry<MonitorEvent.Type, List<Pair<Long, Object>>> entry : typedEvents.entrySet())
    {
      List<Pair<Long, Object>> timeseries = (List)entry.getValue();
      Collections.sort(timeseries, new Comparator()
      {
        public int compare(Pair<Long, Object> o1, Pair<Long, Object> o2)
        {
          return ((Long)o1.first).compareTo((Long)o2.first);
        }
      });
    }
    return typedEvents;
  }
  
  private List<MonitorEvent> getMonitorEvents(UUID compId, UUID serverId, MonitorData historyData, List<String> datumNames, Long startTime, Long endTime)
  {
    List<MonitorEvent> orderedEvents = new ArrayList();
    
    List<MonitorEvent> recentEvents = new ArrayList();
    if (historyData != null) {
      recentEvents = historyData.getHistory();
    }
    if ((startTime != null) && ((recentEvents.isEmpty()) || (((MonitorEvent)recentEvents.get(0)).timeStamp > startTime.longValue())))
    {
      List<MonitorEvent> olderEvents = getDbMonitorEvents(serverId, compId, startTime, endTime, datumNames, "DESC", DEFAULT_MAX_EVENTS);
      orderedEvents.addAll(olderEvents);
    }
    long newestTimestamp = 0L;
    for (MonitorEvent med : orderedEvents) {
      if (med.timeStamp > newestTimestamp) {
        newestTimestamp = med.timeStamp;
      }
    }
    Iterator<MonitorEvent> it = recentEvents.iterator();
    while (it.hasNext())
    {
      MonitorEvent med = (MonitorEvent)it.next();
      if (((newestTimestamp != 0L) && (med.timeStamp <= newestTimestamp)) || ((startTime != null) && (med.timeStamp < startTime.longValue())) || ((endTime != null) && (med.timeStamp > endTime.longValue())) || ((datumNames != null) && (!datumNames.isEmpty()) && (!datumNames.contains(med.type.name())))) {
        it.remove();
      }
    }
    for (MonitorEvent med : recentEvents) {
      orderedEvents.add(med);
    }
    return orderedEvents;
  }
  
  private List<MonitorEvent> getDbMonitorEvents(UUID serverID, UUID entityID, Long startTime, Long endTime, List<String> datumNames, String desc, int default_max_events)
  {
    synchronized (persistChangeLock)
    {
      if (persistMonitor) {
        return getDbMonitorEventsFromES(serverID, entityID, startTime, endTime, datumNames, desc, Integer.valueOf(default_max_events));
      }
    }
    return new ArrayList();
  }
  
  private static String listToString(List<?> list)
  {
    if (list == null) {
      return "";
    }
    String ret = null;
    for (Object obj : list) {
      ret = ret + "," + obj;
    }
    return ret;
  }
  
  private static List<String> stringToList(String string)
  {
    if (string == null) {
      return null;
    }
    if (string.trim().length() == 0) {
      return Collections.emptyList();
    }
    String[] parts = string.split("[,]");
    return Arrays.asList(parts);
  }
  
  private static String getCompStatus(UUID uuid)
    throws MetaDataRepositoryException
  {
    MetaInfo.StatusInfo si = MetadataRepository.getINSTANCE().getStatusInfo(uuid, WASecurityManager.TOKEN);
    if (si != null) {
      return si.status.name();
    }
    return "UNKNOWN";
  }
  
  public static class MonitorStats
    implements Serializable
  {
    private static final long serialVersionUID = 4262383695115160633L;
    public Object min;
    public Object max;
    public Object ave;
    public Object sum;
    public Object count;
    public Object diff;
    public Object resets;
    
    public String toString()
    {
      return "{min:" + this.min + ", max: " + this.max + "ave:" + this.ave + ", sum: " + this.sum + "count:" + this.count + ", diff: " + this.diff + "resets:" + this.resets + "}";
    }
  }
  
  private static MonitorStats calculateStats(String field, List<Pair<Long, Object>> values)
  {
    long min = Long.MAX_VALUE;long max = Long.MIN_VALUE;long ave = 0L;long sum = 0L;long count = 0L;
    long first = -1L;long last = -1L;
    long diff = 0L;
    long resets = 0L;
    boolean isInput = field.contains("input");
    for (Pair<Long, Object> data : values) {
      if ((data.second instanceof Long))
      {
        long value = ((Long)data.second).longValue();
        if (first == -1L) {
          first = value;
        }
        if ((isInput) && (last != -1L) && (value < last))
        {
          diff += last - first;
          first = value;
          resets += 1L;
        }
        last = value;
        if (value < min) {
          min = value;
        }
        if (value > max) {
          max = value;
        }
        sum += value;
        count += 1L;
        ave = sum / count;
      }
      else
      {
        return null;
      }
    }
    if ((last != -1L) && (first != -1L)) {
      diff += last - first;
    }
    MonitorStats stats = new MonitorStats();
    stats.min = Long.valueOf(min);
    stats.max = Long.valueOf(max);
    stats.ave = Long.valueOf(ave);
    stats.sum = Long.valueOf(sum);
    stats.count = Long.valueOf(count);
    stats.diff = Long.valueOf(diff);
    stats.resets = Long.valueOf(resets);
    return stats;
  }
  
  private void addKafkaResults(Map<WactionKey, Map<String, Object>> result)
  {
    ResultKey key = new ResultKey(KAFKA_ENTITY_UUID, KAFKA_SERVER_UUID);
    
    MonitorData historyData = (MonitorData)monitorDataMap.get(new MonitorData.Key(KAFKA_ENTITY_UUID, KAFKA_SERVER_UUID));
    if (historyData == null) {
      return;
    }
    Map<String, Object> newEntResults = new HashMap();
    for (MonitorEvent.Type type : MonitorEvent.KAFKA_TYPES)
    {
      String typeString = type.toString();
      if (historyData.getValues().containsKey(typeString))
      {
        String s = typeString.toLowerCase().replace("_", "-");
        newEntResults.put(s, historyData.getValues().get(typeString));
      }
    }
    newEntResults.put("name", "kafka");
    newEntResults.put("id", KAFKA_ENTITY_UUID);
    
    result.put(key, newEntResults);
  }
  
  private void addElasticsearchResults(Map<WactionKey, Map<String, Object>> result)
  {
    ResultKey key = new ResultKey(ES_ENTITY_UUID, ES_SERVER_UUID);
    
    MonitorData historyData = (MonitorData)monitorDataMap.get(new MonitorData.Key(ES_ENTITY_UUID, ES_SERVER_UUID));
    if (historyData == null) {
      return;
    }
    Map<String, Object> newEntResults = new HashMap();
    for (MonitorEvent.Type type : MonitorEvent.ES_TYPES)
    {
      String typeString = type.toString();
      if (historyData.getValues().containsKey(typeString))
      {
        String s = typeString.toLowerCase().replace("_", "-");
        newEntResults.put(s, historyData.getValues().get(typeString));
      }
    }
    newEntResults.put("name", "es");
    newEntResults.put("id", ES_ENTITY_UUID);
    
    result.put(key, newEntResults);
  }
  
  private void addExternalComponentResults(Map<WactionKey, Map<String, Object>> result, UUID compId, UUID serverId, String name, List<String> timeSeriesFields, List<String> statsFields, List<String> datumNames, Long startTime, Long endTime)
    throws Exception
  {
    MonitorData historyData = (MonitorData)monitorDataMap.get(new MonitorData.Key(compId, serverId));
    ResultKey key = new ResultKey(compId, serverId);
    Map<String, Object> serverCurrentMap = new HashMap();
    Map<MonitorEvent.Type, List<Pair<Long, Object>>> timeSeriesData = null;
    if ((datumNames != null) || (endTime != null)) {
      timeSeriesData = getMonitorEventsAsMap(compId, serverId, historyData, (endTime != null) || ((statsFields != null) && (!statsFields.isEmpty())) || ((timeSeriesFields != null) && (!timeSeriesFields.isEmpty())) ? null : datumNames, startTime, endTime);
    }
    Map<String, Object> cachedServerCurrentMap = null;
    if (historyData != null) {
      cachedServerCurrentMap = historyData.getValues();
    } else {
      cachedServerCurrentMap = new HashMap();
    }
    for (Map.Entry<String, Object> entry : cachedServerCurrentMap.entrySet())
    {
      String newKey = ((String)entry.getKey()).toLowerCase().replaceAll("[_]", "-");
      serverCurrentMap.put(newKey, entry.getValue());
    }
    if (endTime != null)
    {
      Iterator<Map.Entry<MonitorEvent.Type, List<Pair<Long, Object>>>> it = timeSeriesData.entrySet().iterator();
      while (it.hasNext())
      {
        Map.Entry<MonitorEvent.Type, List<Pair<Long, Object>>> entry = (Map.Entry)it.next();
        String newKey = ((MonitorEvent.Type)entry.getKey()).name().toLowerCase().replaceAll("[_]", "-");
        
        List<Pair<Long, Object>> list = (List)entry.getValue();
        if ((list != null) && (!list.isEmpty()))
        {
          serverCurrentMap.put("timestamp", ((Pair)list.get(list.size() - 1)).first);
          
          serverCurrentMap.put(newKey, ((Pair)list.get(list.size() - 1)).second);
        }
      }
    }
    Map<String, Object> newEntResults = new HashMap();
    newEntResults.put("most-recent-data", serverCurrentMap);
    newEntResults.put("name", name);
    newEntResults.put("type", name);
    newEntResults.put("id", compId);
    if ((datumNames != null) && (timeSeriesFields != null))
    {
      Map<String, List<Pair<Long, Object>>> renamedTimeSeriesData = new HashMap();
      for (Map.Entry<MonitorEvent.Type, List<Pair<Long, Object>>> entry : timeSeriesData.entrySet())
      {
        String newKey = ((MonitorEvent.Type)entry.getKey()).name().toLowerCase().replaceAll("[_]", "-");
        if ((timeSeriesFields.isEmpty()) || (timeSeriesFields.contains(newKey))) {
          renamedTimeSeriesData.put(newKey, entry.getValue());
        }
      }
      newEntResults.put("time-series-data", renamedTimeSeriesData);
    }
    if ((datumNames != null) && (statsFields != null))
    {
      Map<String, MonitorStats> statsData = new HashMap();
      for (Map.Entry<MonitorEvent.Type, List<Pair<Long, Object>>> entry : timeSeriesData.entrySet())
      {
        String newKey = ((MonitorEvent.Type)entry.getKey()).name().toLowerCase().replaceAll("[_]", "-");
        if ((statsFields.isEmpty()) || (statsFields.contains(newKey)))
        {
          MonitorStats stats = calculateStats(newKey, (List)entry.getValue());
          if (stats != null) {
            statsData.put(newKey, stats);
          }
        }
      }
      newEntResults.put("stats", statsData);
    }
    result.put(key, newEntResults);
  }
  
  private void addComponentResults(Map<WactionKey, Map<String, Object>> result, Set<MetaInfo.Server> servers, MetaInfo.MetaObject comp, UUID serverId, List<String> timeSeriesFields, List<String> statsFields, List<String> datumNames, Long startTime, Long endTime)
    throws Exception
  {
    MonitorData historyData = (MonitorData)monitorDataMap.get(new MonitorData.Key(comp.uuid, serverId));
    ResultKey key = new ResultKey(comp.uuid, serverId);
    Map<String, Object> serverCurrentMap = new HashMap();
    Map<MonitorEvent.Type, List<Pair<Long, Object>>> timeSeriesData = null;
    if ((datumNames != null) || (endTime != null)) {
      timeSeriesData = getMonitorEventsAsMap(comp.uuid, serverId, historyData, (endTime != null) || ((statsFields != null) && (!statsFields.isEmpty())) || ((timeSeriesFields != null) && (!timeSeriesFields.isEmpty())) ? null : datumNames, startTime, endTime);
    }
    Map<String, Object> cachedServerCurrentMap = null;
    if (historyData != null) {
      cachedServerCurrentMap = historyData.getValues();
    } else {
      cachedServerCurrentMap = new HashMap();
    }
    for (Map.Entry<String, Object> entry : cachedServerCurrentMap.entrySet())
    {
      String newKey = ((String)entry.getKey()).toLowerCase().replaceAll("[_]", "-");
      serverCurrentMap.put(newKey, entry.getValue());
      if (((String)entry.getKey()).equals("CPU_RATE"))
      {
        long rawRate = ((Long)entry.getValue()).longValue();
        serverCurrentMap.put("cpu", renderCpuPercent(rawRate));
        serverCurrentMap.put("cpu-per-node", renderCpuPerNodePercent(rawRate, Runtime.getRuntime().availableProcessors()));
      }
    }
    if (endTime != null)
    {
      Iterator<Map.Entry<MonitorEvent.Type, List<Pair<Long, Object>>>> it = timeSeriesData.entrySet().iterator();
      while (it.hasNext())
      {
        Map.Entry<MonitorEvent.Type, List<Pair<Long, Object>>> entry = (Map.Entry)it.next();
        String newKey = ((MonitorEvent.Type)entry.getKey()).name().toLowerCase().replaceAll("[_]", "-");
        List<Pair<Long, Object>> list = (List)entry.getValue();
        if ((list != null) && (!list.isEmpty()))
        {
          serverCurrentMap.put("timestamp", ((Pair)list.get(list.size() - 1)).first);
          serverCurrentMap.put(newKey, ((Pair)list.get(list.size() - 1)).second);
        }
      }
    }
    Map<String, Object> newEntResults = new HashMap();
    newEntResults.put("most-recent-data", serverCurrentMap);
    newEntResults.put("name", comp.name);
    newEntResults.put("id", comp.uuid);
    newEntResults.put("ns-name", comp.nsName);
    newEntResults.put("full-name", comp.nsName + "." + comp.name);
    newEntResults.put("ns-id", comp.namespaceId);
    if ((comp instanceof MetaInfo.Server))
    {
      MetaInfo.Server server = (MetaInfo.Server)comp;
      String apps = null;
      int numComps = 0;
      for (Map.Entry<String, Set<UUID>> entry : server.currentUUIDs.entrySet())
      {
        apps = apps + "," + (String)entry.getKey();
        numComps += ((Set)entry.getValue()).size();
      }
      newEntResults.put("type", server.isAgent ? EntityType.AGENT : comp.type);
      newEntResults.put("status", "RUNNING");
      newEntResults.put("servers", comp.name);
      newEntResults.put("num-servers", Integer.valueOf(1));
      newEntResults.put("applications", apps);
      newEntResults.put("num-applications", Integer.valueOf(server.currentUUIDs.entrySet().size()));
      newEntResults.put("num-components", Integer.valueOf(numComps));
    }
    else
    {
      List<String> onServerList = new ArrayList();
      MetaInfo.Server server;
      for (Iterator i$ = servers.iterator(); i$.hasNext();)
      {
        server = (MetaInfo.Server)i$.next();
        if ((MonitorEvent.RollupUUID.equals(serverId)) || (server.uuid.equals(serverId)))
        {
          Map<String, Set<UUID>> appUUIDs = server.getCurrentUUIDs();
          for (Map.Entry<String, Set<UUID>> entry : appUUIDs.entrySet()) {
            if (((Set)entry.getValue()).contains(comp.uuid))
            {
              onServerList.add(server.name);
              break;
            }
          }
        }
      }
      
      newEntResults.put("type", comp.type);
      if (comp.type == EntityType.APPLICATION) {
        newEntResults.put("status", getCompStatus(comp.uuid));
      } else {
        newEntResults.put("status", "");
      }
      newEntResults.put("servers", listToString(onServerList));
      newEntResults.put("num-servers", Integer.valueOf(onServerList.size()));
      MetaInfo.Flow app = null;
      try
      {
        app = comp.getCurrentApp();
      }
      catch (MetaDataRepositoryException e) {}
      newEntResults.put("applications", app != null ? app.getFullName() : "<NONE>");
      newEntResults.put("num-applications", Integer.valueOf(app != null ? 1 : 0));
      int numComps = 1;
      if ((comp instanceof MetaInfo.Flow))
      {
        MetaInfo.Flow flow = (MetaInfo.Flow)comp;
        numComps = 0;
        for (Map.Entry<EntityType, LinkedHashSet<UUID>> entry : flow.objects.entrySet()) {
          if (((EntityType)entry.getKey()).equals(EntityType.FLOW)) {
            for (UUID subUUID : (LinkedHashSet)entry.getValue())
            {
              MetaInfo.Flow subFlow = (MetaInfo.Flow)MetadataRepository.getINSTANCE().getMetaObjectByUUID(subUUID, WASecurityManager.TOKEN);
              if (subFlow != null) {
                for (Map.Entry<EntityType, LinkedHashSet<UUID>> subEntry : subFlow.objects.entrySet()) {
                  numComps += ((LinkedHashSet)subEntry.getValue()).size();
                }
              }
            }
          } else {
            numComps += ((LinkedHashSet)entry.getValue()).size();
          }
        }
      }
      newEntResults.put("num-components", Integer.valueOf(numComps));
      if ((app != null) && (app.getMetaInfoStatus() != null)) {
        newEntResults.put("is-valid", Boolean.valueOf(app.getMetaInfoStatus().isValid()));
      }
    }
    if ((datumNames != null) && (timeSeriesFields != null))
    {
      Map<String, List<Pair<Long, Object>>> renamedTimeSeriesData = new HashMap();
      for (Map.Entry<MonitorEvent.Type, List<Pair<Long, Object>>> entry : timeSeriesData.entrySet())
      {
        String newKey = ((MonitorEvent.Type)entry.getKey()).name().toLowerCase().replaceAll("[_]", "-");
        if ((timeSeriesFields.isEmpty()) || (timeSeriesFields.contains(newKey))) {
          renamedTimeSeriesData.put(newKey, entry.getValue());
        }
        if ((MonitorEvent.Type.CPU_RATE.equals(entry.getKey())) && (
          (timeSeriesFields.isEmpty()) || (timeSeriesFields.contains("cpu-per-node"))))
        {
          List<Pair<Long, Object>> cpuRates = new ArrayList();
          List<Pair<Long, Object>> cpuRatesPerNode = new ArrayList();
          for (Pair<Long, Object> rawCpu : (List)entry.getValue())
          {
            String cpuPercentString = renderCpuPercent(((Long)rawCpu.second).longValue());
            String cpuPerNodePercentString = renderCpuPerNodePercent(((Long)rawCpu.second).longValue(), Runtime.getRuntime().availableProcessors());
            
            cpuRates.add(Pair.make(rawCpu.first, cpuPercentString));
            cpuRatesPerNode.add(Pair.make(rawCpu.first, cpuPerNodePercentString));
          }
          renamedTimeSeriesData.put("cpu", cpuRates);
          renamedTimeSeriesData.put("cpu-per-node", cpuRatesPerNode);
        }
      }
      newEntResults.put("time-series-data", renamedTimeSeriesData);
    }
    if ((datumNames != null) && (statsFields != null))
    {
      Map<String, MonitorStats> statsData = new HashMap();
      for (Map.Entry<MonitorEvent.Type, List<Pair<Long, Object>>> entry : timeSeriesData.entrySet())
      {
        String newKey = ((MonitorEvent.Type)entry.getKey()).name().toLowerCase().replaceAll("[_]", "-");
        if ((statsFields.isEmpty()) || (statsFields.contains(newKey)))
        {
          MonitorStats stats = calculateStats(newKey, (List)entry.getValue());
          if (stats != null) {
            statsData.put(newKey, stats);
          }
        }
        if ((MonitorEvent.Type.CPU_RATE.equals(entry.getKey())) && (
          (statsFields.isEmpty()) || (statsFields.contains("cpu-per-node"))))
        {
          MonitorStats stats = calculateStats("cpu", (List)entry.getValue());
          if (stats != null)
          {
            stats.min = renderCpuPercent(((Long)stats.min).longValue());
            stats.max = renderCpuPercent(((Long)stats.max).longValue());
            stats.ave = renderCpuPercent(((Long)stats.ave).longValue());
            statsData.put("cpu", stats);
          }
        }
      }
      newEntResults.put("stats", statsData);
    }
    result.put(key, newEntResults);
  }
  
  private void addApplicationResults(Map<WactionKey, Map<String, Object>> result, Set<MetaInfo.Server> servers, MetaInfo.Flow app, UUID serverId, List<String> timeSeriesFields, List<String> statsFields, List<String> datumNames, Long startTime, Long endTime)
    throws Exception
  {
    Map<EntityType, LinkedHashSet<UUID>> appE = app.getObjects();
    if (!appE.isEmpty()) {
      for (Map.Entry<EntityType, LinkedHashSet<UUID>> appList : appE.entrySet())
      {
        for (UUID uuid : (LinkedHashSet)appList.getValue())
        {
          MetaInfo.MetaObject subObj = MetadataRepository.getINSTANCE().getMetaObjectByUUID(uuid, WASecurityManager.TOKEN);
          if (subObj != null) {
            addComponentResults(result, servers, subObj, serverId, timeSeriesFields, statsFields, datumNames, startTime, endTime);
          }
        }
        if (((EntityType)appList.getKey()).equals(EntityType.FLOW)) {
          for (UUID flowUUID : (LinkedHashSet)appList.getValue())
          {
            MetaInfo.Flow flow = (MetaInfo.Flow)MetadataRepository.getINSTANCE().getMetaObjectByUUID(flowUUID, WASecurityManager.TOKEN);
            if (flow != null)
            {
              Map<EntityType, LinkedHashSet<UUID>> flowE = flow.getObjects();
              for (Map.Entry<EntityType, LinkedHashSet<UUID>> flowList : flowE.entrySet()) {
                for (UUID uuid : (LinkedHashSet)flowList.getValue())
                {
                  MetaInfo.MetaObject subObj = MetadataRepository.getINSTANCE().getMetaObjectByUUID(uuid, WASecurityManager.TOKEN);
                  if (subObj != null) {
                    addComponentResults(result, servers, subObj, serverId, timeSeriesFields, statsFields, datumNames, startTime, endTime);
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  
  private static List<String> getTimeSeriesFields(List<String> flist, Map<String, Object> filter)
  {
    boolean getTimeSeries = flist.contains("time-series");
    if (!getTimeSeries) {
      return null;
    }
    String timeSeriesFields = (String)filter.get("time-series-fields");
    List<String> timeSeriesFieldList = stringToList(timeSeriesFields);
    if ((getTimeSeries) && (timeSeriesFieldList == null)) {
      timeSeriesFieldList = Collections.emptyList();
    }
    return timeSeriesFieldList;
  }
  
  private static List<String> getStatsFields(List<String> flist, Map<String, Object> filter)
  {
    boolean getStats = flist.contains("stats");
    if (!getStats) {
      return null;
    }
    String statsFields = (String)filter.get("stats-fields");
    List<String> statsFieldList = stringToList(statsFields);
    if ((getStats) && (statsFieldList == null)) {
      statsFieldList = Collections.emptyList();
    }
    return statsFieldList;
  }
  
  private static List<String> getDatumNamesFromFilter(List<String> flist, Map<String, Object> filter)
  {
    List<String> timeSeriesFieldList = getTimeSeriesFields(flist, filter);
    List<String> statsFieldList = getStatsFields(flist, filter);
    
    List<String> allFields = null;
    if ((timeSeriesFieldList != null) || (statsFieldList != null)) {
      allFields = new ArrayList();
    }
    if (timeSeriesFieldList != null) {
      allFields.addAll(timeSeriesFieldList);
    }
    if (statsFieldList != null) {
      allFields.addAll(statsFieldList);
    }
    if (allFields == null) {
      return null;
    }
    List<String> datumNames = new ArrayList();
    if (allFields.contains("cpu-per-node")) {
      allFields.add("cpu-rate");
    }
    for (String string : allFields)
    {
      String val = string.toUpperCase().replaceAll("[-]", "_");
      try
      {
        MonitorEvent.Type.valueOf(val);
        datumNames.add(val);
      }
      catch (Exception e) {}
    }
    return datumNames;
  }
  
  private static Long getStartTimeFromFilter(Map<String, Object> filter)
  {
    Long startTime = (Long)filter.get("startTime");
    if (startTime == null) {
      startTime = (Long)filter.get("start-time");
    }
    return startTime;
  }
  
  private static Long getEndTimeFromFilter(Map<String, Object> filter)
  {
    Long endTime = (Long)filter.get("endTime");
    if (endTime == null) {
      endTime = (Long)filter.get("end-time");
    }
    return endTime;
  }
  
  private static UUID getAppIDFromFilter(Map<String, Object> filter, AuthToken authToken)
    throws MetaDataRepositoryException
  {
    String appName = (String)filter.get("app-name");
    String appUUID = (String)filter.get("app-uuid");
    
    UUID appId = null;
    if (appUUID != null)
    {
      MetaInfo.Flow app = (MetaInfo.Flow)MetadataRepository.getINSTANCE().getMetaObjectByUUID(new UUID(appUUID), authToken);
      if (app == null) {
        throw new RuntimeException("App with UUID " + appUUID + " cannot be found");
      }
      appId = app.uuid;
    }
    else if (appName != null)
    {
      MetaInfo.Flow app;
      if (appName.indexOf(".") != -1)
      {
        String[] namespaceAndName = appName.split("\\.");
        assert ((namespaceAndName[0] != null) && (namespaceAndName[1] != null));
        app = (MetaInfo.Flow)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.APPLICATION, namespaceAndName[0], namespaceAndName[1], null, authToken);
      }
      else
      {
        app = (MetaInfo.Flow)MetadataRepository.getINSTANCE().getMetaObjectByName(null, null, appName, null, authToken);
      }
      if (app == null) {
        throw new RuntimeException("App with name " + appName + " cannot be found");
      }
      appId = app.uuid;
    }
    return appId;
  }
  
  private static UUID getServerIDFromFilter(Set<MetaInfo.Server> servers, Map<String, Object> filter)
  {
    UUID serverId = MonitorEvent.RollupUUID;
    String serverName = (String)filter.get("server-name");
    String serverUUID = (String)filter.get("server-uuid");
    if ((serverUUID == null) && 
      ((filter.get("serverInfo") instanceof UUID))) {
      serverUUID = filter.get("serverInfo").toString();
    }
    if (KAFKA_SERVER_UUID.toString().equals(serverUUID)) {
      return KAFKA_SERVER_UUID;
    }
    if (ES_SERVER_UUID.toString().equals(serverUUID)) {
      return ES_SERVER_UUID;
    }
    if ((serverName != null) || (serverUUID != null)) {
      for (MetaInfo.Server server : servers) {
        if ((server.name.equals(serverName)) || (server.uuid.toString().equals(serverUUID))) {
          return server.uuid;
        }
      }
    }
    return serverId;
  }
  
  private static void syncTimeSeriesData(Map<WactionKey, Map<String, Object>> results)
  {
    List<Long> allTimePeriods = new ArrayList();
    for (Map.Entry<WactionKey, Map<String, Object>> entry : results.entrySet())
    {
      Map<String, Object> data = (Map)entry.getValue();
      Map<String, List<Pair<Long, Object>>> timeSeriesData = (Map)data.get("time-series-data");
      if (timeSeriesData != null) {
        for (Map.Entry<String, List<Pair<Long, Object>>> timeSeriesEntry : timeSeriesData.entrySet())
        {
          List<Pair<Long, Object>> timeSeriesList = (List)timeSeriesEntry.getValue();
          for (Pair<Long, Object> timeSeriesValue : timeSeriesList) {
            if (!allTimePeriods.contains(timeSeriesValue.first)) {
              allTimePeriods.add(timeSeriesValue.first);
            }
          }
        }
      }
    }
    if (allTimePeriods.isEmpty()) {
      return;
    }
    Collections.sort(allTimePeriods);
    Map<String, List<Pair<Long, Object>>> timeSeriesData;

    for (Map.Entry<WactionKey, Map<String, Object>> entry : results.entrySet())
    {
      Map<String, Object> data = (Map)entry.getValue();
      timeSeriesData = (Map)data.get("time-series-data");
      if (timeSeriesData != null) {
        for (String timeSeriesEntry : timeSeriesData.keySet())
        {
          Map<Long, Object> timeSeriesMap = new HashMap();
          List<Pair<Long, Object>> timeSeriesList = (List)timeSeriesData.get(timeSeriesEntry);
          Object firstValue = null;
          for (Pair<Long, Object> timeSeriesValue : timeSeriesList)
          {
            timeSeriesMap.put(timeSeriesValue.first, timeSeriesValue.second);
            if (firstValue == null) {
              firstValue = timeSeriesValue.second;
            }
          }
          Object prevValue = (firstValue instanceof String) ? "" : Long.valueOf(0L);
          List<Pair<Long, Object>> updatedTimeSeriesList = new ArrayList();
          for (Long timePeriod : allTimePeriods)
          {
            Object value = timeSeriesMap.get(timePeriod);
            if (value == null) {
              value = prevValue;
            }
            updatedTimeSeriesList.add(Pair.make(timePeriod, value));
            prevValue = value;
          }
          timeSeriesData.put(timeSeriesEntry, updatedTimeSeriesList);
        }
      }
    }
  }
  
  public Map<WactionKey, Map<String, Object>> getAllMonitorWactions(String[] fields, Map<String, Object> filter, AuthToken authToken)
    throws Exception
  {
    Map<WactionKey, Map<String, Object>> result = new LinkedHashMap();
    if (filter == null) {
      return result;
    }
    List<String> flist = fields == null ? Collections.EMPTY_LIST : Arrays.asList(fields);
    
    boolean rollupApps = flist.contains("rollupApps");
    
    Long startTime = getStartTimeFromFilter(filter);
    Long endTime = getEndTimeFromFilter(filter);
    
    List<String> timeSeriesFields = getTimeSeriesFields(flist, filter);
    List<String> statsFields = getStatsFields(flist, filter);
    
    List<String> datumNames = getDatumNamesFromFilter(flist, filter);
    UUID appId = getAppIDFromFilter(filter, authToken);
    
    Set<MetaInfo.MetaObject> allAppsMo = MetadataRepository.getINSTANCE().getByEntityType(EntityType.APPLICATION, authToken);
    List<MetaInfo.Flow> allApps = new ArrayList();
    for (MetaInfo.MetaObject obj : allAppsMo) {
      allApps.add((MetaInfo.Flow)obj);
    }
    Set<MetaInfo.Server> servers = MetadataRepository.getINSTANCE().getByEntityType(EntityType.SERVER, WASecurityManager.TOKEN);
    UUID serverId = getServerIDFromFilter(servers, filter);
    if (rollupApps)
    {
      for (MetaInfo.Flow app : allApps) {
        if ((appId == null) || (app.uuid.equals(appId))) {
          addComponentResults(result, servers, app, serverId, timeSeriesFields, statsFields, datumNames, startTime, endTime);
        }
      }
      for (MetaInfo.Server server : servers) {
        if ((serverId.equals(MonitorEvent.RollupUUID)) || (server.uuid.equals(serverId))) {
          addComponentResults(result, servers, server, server.uuid, timeSeriesFields, statsFields, datumNames, startTime, endTime);
        }
      }
      if (flist.contains("es")) {
        addExternalComponentResults(result, ES_ENTITY_UUID, ES_SERVER_UUID, "es", timeSeriesFields, statsFields, datumNames, startTime, endTime);
      }
      if (flist.contains("kafka")) {
        addExternalComponentResults(result, KAFKA_ENTITY_UUID, KAFKA_SERVER_UUID, "kafka", timeSeriesFields, statsFields, datumNames, startTime, endTime);
      }
    }
    else
    {
      for (MetaInfo.Flow app : allApps) {
        if ((appId == null) || (app.uuid.equals(appId))) {
          addApplicationResults(result, servers, app, serverId, timeSeriesFields, statsFields, datumNames, startTime, endTime);
        }
      }
    }
    syncTimeSeriesData(result);
    
    return result;
  }
  
  public Map<WactionKey, Map<String, Object>> getMonitorWaction(WactionKey wk, String[] fields, Map<String, Object> filter)
    throws Exception
  {
    Map<WactionKey, Map<String, Object>> result = new LinkedHashMap();
    
    Set<MetaInfo.Server> servers = MetadataRepository.getINSTANCE().getByEntityType(EntityType.SERVER, WASecurityManager.TOKEN);
    UUID serverId = getServerIDFromFilter(servers, filter);
    
    Long startTime = getStartTimeFromFilter(filter);
    Long endTime = getEndTimeFromFilter(filter);
    
    List<String> flist = fields == null ? Collections.EMPTY_LIST : Arrays.asList(fields);
    boolean getTimeSeries = flist.contains("time-series");
    boolean getStats = flist.contains("stats");
    
    List<String> timeSeriesFields = getTimeSeriesFields(flist, filter);
    List<String> statsFields = getStatsFields(flist, filter);
    
    List<String> datumNames = getDatumNamesFromFilter(flist, filter);
    if (wk != null)
    {
      UUID entityId = wk.id;
      MetaInfo.MetaObject obj = MetadataRepository.getINSTANCE().getMetaObjectByUUID(entityId, WASecurityManager.TOKEN);
      if (obj != null)
      {
        if ((obj instanceof MetaInfo.Server)) {
          addComponentResults(result, servers, obj, obj.uuid, timeSeriesFields, statsFields, datumNames, startTime, endTime);
        } else {
          addComponentResults(result, servers, obj, serverId, timeSeriesFields, statsFields, datumNames, startTime, endTime);
        }
      }
      else if (KAFKA_ENTITY_UUID.equals(entityId)) {
        addKafkaResults(result);
      } else if (ES_ENTITY_UUID.equals(entityId)) {
        addElasticsearchResults(result);
      } else if (logger.isInfoEnabled()) {
        logger.info("Component not found for Monitor Waction request Entity UUID=" + entityId + " Server UUID=" + serverId);
      }
    }
    syncTimeSeriesData(result);
    
    return result;
  }
  
  public static boolean monitorIsEnabled()
  {
    if (HazelcastSingleton.isAvailable())
    {
      Object enableMonitor = HazelcastSingleton.get().getMap("#ClusterSettings").get("com.bloom.config.enable-monitor");
      if (enableMonitor != null) {
        return enableMonitor.toString().equalsIgnoreCase("TRUE");
      }
    }
    String localProp = System.getProperty("com.bloom.config.enable-monitor");
    Boolean b = new Boolean((localProp != null) && (localProp.equalsIgnoreCase("TRUE")));
    if (HazelcastSingleton.isAvailable()) {
      HazelcastSingleton.get().getMap("#ClusterSettings").put("com.bloom.config.enable-monitor", b);
    }
    return b.booleanValue();
  }
  
  public static boolean monitorPersistenceIsEnabled()
  {
    if (HazelcastSingleton.isAvailable())
    {
      Object enableMonitor = HazelcastSingleton.get().getMap("#ClusterSettings").get("com.bloom.config.enable-monitor-persistence");
      if (enableMonitor != null) {
        return !enableMonitor.toString().equalsIgnoreCase("FALSE");
      }
    }
    String localProp = System.getProperty("com.bloom.config.enable-monitor-persistence");
    Boolean b = new Boolean((localProp == null) || (!localProp.equalsIgnoreCase("FALSE")));
    if (HazelcastSingleton.isAvailable()) {
      HazelcastSingleton.get().getMap("#ClusterSettings").put("com.bloom.config.enable-monitor-persistence", b);
    }
    return b.booleanValue();
  }
  
  public static int getDbExpireTime()
  {
    if (HazelcastSingleton.isAvailable())
    {
      Object hazProp = HazelcastSingleton.get().getMap("#ClusterSettings").get("com.bloom.config.monitor-db-max");
      if (hazProp != null) {
        try
        {
          Integer result = (Integer)hazProp;
          return result.intValue();
        }
        catch (ClassCastException e)
        {
          logger.error(e);
        }
      }
    }
    String localProp = System.getProperty("com.bloom.config.monitor-db-max");
    if ((localProp != null) && (!localProp.isEmpty()))
    {
      Integer result = Integer.valueOf(localProp);
      if (HazelcastSingleton.isAvailable()) {
        HazelcastSingleton.get().getMap("#ClusterSettings").put("com.bloom.config.monitor-db-max", result);
      }
      return result.intValue();
    }
    return DEFAULT_DB_EXPIRE_TIME;
  }
  
  public static void resetDbConnection()
  {
    theDb = null;
  }
  
  public void close()
    throws Exception
  {
    super.close();
    if (logger.isInfoEnabled()) {
      logger.info("Waiting until bloom Monitor has completed last task");
    }
    running = false;
    if (this.executor != null) {
      try
      {
        if (!this.executor.awaitTermination(5L, TimeUnit.SECONDS))
        {
          this.executor.shutdownNow();
          if (!this.executor.awaitTermination(5L, TimeUnit.SECONDS)) {
            System.err.println("PeriodicPersistence_Scheduler did not terminate");
          }
        }
      }
      catch (InterruptedException ie)
      {
        this.executor.shutdown();
        Thread.currentThread().interrupt();
      }
    }
    monitorDataMap.clear();
    if (logger.isInfoEnabled()) {
      logger.info("bloom Monitor shut down");
    }
  }
  
  private static List<MonitorEvent> getDbMonitorEventsFromDerby(UUID serverID, UUID entityID, Long startTime, Long endTime, Collection<String> datumNames, String orderBy, Integer maxEvents)
  {
    PersistenceLayer db = getDb();
    if (db == null) {
      return new ArrayList();
    }
    try
    {
      Map<String, Object> params = new HashMap();
      if (serverID != null) {
        params.put("serverID", serverID);
      }
      if (entityID != null) {
        params.put("entityID", entityID);
      }
      if (startTime != null) {
        params.put("startTime", startTime);
      }
      if (endTime != null) {
        params.put("endTime", endTime);
      }
      if ((datumNames != null) && (!datumNames.isEmpty()))
      {
        List<MonitorEvent.Type> enumDatumNames = new ArrayList();
        for (String datum : datumNames) {
          try
          {
            MonitorEvent.Type enumDatumName = MonitorEvent.Type.valueOf(datum);
            enumDatumNames.add(enumDatumName);
          }
          catch (Throwable t)
          {
            logger.error("Invalid datum name: " + datum + " specified in monitor query");
          }
        }
        params.put("datumNames", enumDatumNames);
      }
      String queryGetMonitorEvents = "SELECT     se FROM     MonitorEvent se " + getWhereClause(params) + "ORDER BY se.timeStamp " + orderBy + " ";
      
      List<MonitorEvent> l = db.runQuery(queryGetMonitorEvents, params, maxEvents);
      if (l == null) {}
      return new ArrayList();
    }
    catch (Exception e)
    {
      logger.error("Could not get monitor data", e);
    }
    return new ArrayList();
  }
  
  private static List<MonitorEvent> getDbMonitorEventsFromES(UUID serverID, UUID entityID, Long startTime, Long endTime, Collection<String> datumNames, String orderBy, Integer maxEvents)
  {
    List<MonitorEvent> result = new ArrayList();
    try
    {
      Map<String, Object> params = new HashMap();
      if (serverID != null) {
        params.put("serverID", serverID);
      }
      if (entityID != null) {
        params.put("entityID", entityID);
      }
      if (startTime != null) {
        params.put("startTime", startTime);
      }
      if (endTime != null) {
        params.put("endTime", endTime);
      }
      if ((datumNames != null) && (!datumNames.isEmpty()))
      {
        List<MonitorEvent.Type> enumDatumNames = new ArrayList();
        for (String datum : datumNames) {
          try
          {
            MonitorEvent.Type enumDatumName = MonitorEvent.Type.valueOf(datum);
            enumDatumNames.add(enumDatumName);
          }
          catch (Throwable t)
          {
            logger.error("Invalid datum name: " + datum + " specified in monitor query");
          }
        }
        params.put("datumNames", enumDatumNames);
      }
      String isAscending = "true";
      if (orderBy.equals("DESC")) {
        isAscending = "false";
      }
      String queryGetMonitorEventsWithFilter = "{ \"select\":[ \"" + MonitorEvent.getFields() + "\" ]," + "  \"from\":  [ \"" + "$Internal.MONITORING" + "\" ]" + MonitorEvent.getWhereClauseJson(params) + MonitorEvent.getOrderByClause(isAscending) + '}';
      
      ObjectNode queryJson = (ObjectNode)Utility.readTree(queryGetMonitorEventsWithFilter);
      WActionStoreManager manager = monitorEventDataType.getWActionStore().getManager();
      WActionQuery query = manager.prepareQuery(queryJson);
      Iterator<WAction> checkpoints = query.execute().iterator();
      while (checkpoints.hasNext())
      {
        if (result.size() >= maxEvents.intValue()) {
          break;
        }
        WAction wAction = (WAction)checkpoints.next();
        result.add(new MonitorEvent(wAction));
      }
    }
    catch (WActionStoreMissingException exception)
    {
      String monitoringStoreName = InternalType.MONITORING.getWActionStoreName();
      if (!monitoringStoreName.equals(exception.wActionStoreName)) {
        throw exception;
      }
    }
    return result;
  }
  
  private static void writeToCache(List<MonitorEvent> events)
  {
    Map<MonitorData.Key, MonitorData> entityServerValues = new HashMap();
    for (MonitorEvent event : events)
    {
      MonitorData.Key key = new MonitorData.Key(event.entityID, event.serverID);
      MonitorData data = (MonitorData)entityServerValues.get(key);
      if (data == null)
      {
        data = new MonitorData(event.entityID, event.serverID);
        entityServerValues.put(data.key, data);
      }
      data.addSingleValue(event.type.name(), event.valueString != null ? event.valueString : event.valueLong);
      data.addSingleEvent(event);
      data.addSingleValue("timeStamp", Long.valueOf(event.timeStamp));
    }
    addCurrentMonitorValues(entityServerValues);
  }
  
  private static void queueForCommitToDisk(List<MonitorEvent> se)
  {
    insertQueue.addLast(se);
  }
  
  private static void persistInES(List<MonitorEvent> events)
  {
    long startTime = System.currentTimeMillis();
    List<WAction> wActions = makeMonitoringWactions(events);
    monitorEventDataType.insert(wActions, null);
    if (logger.isDebugEnabled())
    {
      long timeTaken = System.currentTimeMillis() - startTime;
      logger.debug("time taken to insert " + events.size() + " Monitor wactions in ES: " + timeTaken + " ms");
    }
  }
  
  private static void persistHealthRecordInES(HealthRecord events)
  {
    long startTime = System.currentTimeMillis();
    WAction wActions = events.getwAction();
    healthRecordDataType.insert(wActions);
    if (logger.isDebugEnabled())
    {
      long timeTaken = System.currentTimeMillis() - startTime;
      logger.debug("time taken to insert 1 Health wactions in ES: " + timeTaken + " ms");
    }
    lastPersistTime = startTime;
  }
  
  private static List<WAction> makeMonitoringWactions(List<MonitorEvent> events)
  {
    List<WAction> wActions = new ArrayList();
    for (MonitorEvent event : events) {
      wActions.add(event.getwAction());
    }
    return wActions;
  }
  
  private static volatile PersistenceLayer theDb = null;
  
  private static PersistenceLayer getDb()
  {
    if (theDb == null) {
      synchronized (PersistenceLayer.class)
      {
        if (theDb == null) {
          if (Server.persistenceIsEnabled()) {
            try
            {
              if (logger.isDebugEnabled()) {
                logger.debug("bloom Monitor is ON, testing connection...");
              }
              Map<String, MetaInfo.Initializer> startUpMap = HazelcastSingleton.get().getMap("#startUpMap");
              String clusterName = HazelcastSingleton.getClusterName();
              MetaInfo.Initializer initializer = (MetaInfo.Initializer)startUpMap.get(clusterName);
              
              String DBUname = initializer.MetaDataRepositoryUname;
              String DBPassword = initializer.MetaDataRepositoryPass;
              String DBName = initializer.MetaDataRepositoryDBname;
              String DBLocation = initializer.MetaDataRepositoryLocation;
              
              Map<String, Object> props = new HashMap();
              if ((DBUname != null) && (!DBUname.isEmpty())) {
                props.put("javax.persistence.jdbc.user", DBUname);
              }
              if ((DBPassword != null) && (!DBPassword.isEmpty())) {
                props.put("javax.persistence.jdbc.password", DBPassword);
              }
              if ((DBName != null) && (!DBName.isEmpty()) && (DBUname != null) && (!DBUname.isEmpty()) && (DBPassword != null) && (!DBPassword.isEmpty()) && (DBLocation != null) && (!DBLocation.isEmpty())) {
                props.put("javax.persistence.jdbc.url", "jdbc:derby://" + DBLocation + "/" + DBName + ";user=" + DBUname + ";password=" + DBPassword);
              }
              PersistenceLayer pl = PersistenceFactory.createPersistenceLayer("derby-server-monitor", PersistenceFactory.PersistingPurpose.MONITORING, "derby-server-monitor", props);
              pl.init("derby-server-monitor");
              
              theDb = pl;
            }
            catch (PersistenceUnitLoadingException e)
            {
              logger.error("Error initializing the bloom Monitor persistence unit, which indicates a bad configuration", e);
            }
            catch (Exception e)
            {
              logger.error("Unexpected error when building database connection: " + e.getMessage());
            }
          } else if (logger.isInfoEnabled()) {
            logger.info("bloom Monitor is OFF; turn it on by specifying com.bloom.config.enable-monitor=True");
          }
        }
      }
    }
    return theDb;
  }
  
  public List<MonitorEvent> getMonitorEventsSinceLastHealthReport(long endTime)
  {
    HealthMonitor service = new HealthMonitorImpl();
    try
    {
      HealthRecordCollection c = service.getHealthRecordsByCount(1, 0L);
      if ((c == null) || (c.healthRecords.size() < 1)) {
        return null;
      }
      WAction next = (WAction)c.healthRecords.iterator().next();
      JsonNode prevReportEndTimeNode = next.get("endTime");
      long startTime = prevReportEndTimeNode.asLong();
      
      HealthRecordCollection hrc = service.getHealthRecordsByTime(startTime, endTime);
      Set<WAction> hrcEvents = hrc.healthRecords;
      List<MonitorEvent> result = new ArrayList();
      for (WAction w : hrcEvents)
      {
        result.add(new MonitorEvent(w));
        if (Logger.getLogger("Monitor").isDebugEnabled()) {
          Logger.getLogger("Monitor").debug("Number of Monitor Events since previous Health Report: " + result.size());
        }
      }
      return result;
    }
    catch (SearchPhaseExecutionException spee)
    {
      if (spee.shardFailures().length > 0)
      {
        StringBuilder reason = new StringBuilder();
        int ii = 0;
        for (ShardSearchFailure ssf : spee.shardFailures()) {
          if (ssf.reason() != null) {
            reason.append(ii++).append(". ").append(ssf.reason() + "\n\n");
          }
        }
        logger.info("Take a chill pill, don't panic! expected to occur until the first health record has been written out");
        logger.info(reason.toString());
      }
      return null;
    }
    catch (Exception e)
    {
      logger.error(e.getMessage(), e);
    }
    return null;
  }
  
  private void rollHealthReport(long now)
    throws Exception
  {
    MDRepository clientOps = new MDClientOps(HazelcastSingleton.get().getName());
    try
    {
      Set<MetaInfo.Flow> mos = clientOps.getByEntityType(EntityType.APPLICATION, WASecurityManager.TOKEN);
      Map<String, AppHealth> appHealthMap = new HashMap();
      for (MetaInfo.MetaObject mo : mos) {
        if ((!mo.getName().equals("MonitoringProcessApp")) && (!mo.getName().equals("MonitoringSourceApp")) && 
        
          (!mo.getMetaInfoStatus().isAdhoc()) && (!mo.getMetaInfoStatus().isAnonymous()))
        {
          String status = getCompStatus(mo.uuid);
          AppHealth appHealth = new AppHealth(mo.getFullName(), status, mo.getCtime());
          appHealthMap.put(mo.getFullName(), appHealth);
        }
      }
      this.currentHealthReportBuilder.setAppHealthMap(appHealthMap);
    }
    catch (MetaDataRepositoryException e1)
    {
      logger.error("Failed to create app health details for health report", e1);
    }
    try
    {
      int clusterSize = HazelcastSingleton.get().getCluster().getMembers().size();
      this.currentHealthReportBuilder.setClusterSize(clusterSize);
      
      Set agentSet = this.mdRepository.getByEntityType(EntityType.AGENT, WASecurityManager.TOKEN);
      int agentCount = agentSet != null ? agentSet.size() : 0;
      this.currentHealthReportBuilder.setAgentCount(agentCount);
    }
    catch (MetaDataRepositoryException mde)
    {
      logger.error(mde.getMessage(), mde);
    }
    catch (Exception e)
    {
      logger.error("Unable to determine Hazelcast cluster size for Health Record");
    }
    MetaInfo.Initializer initializer = getInitializerObject();
    boolean isCxnAlive = this.metaDataDBDetails.isConnectionAlive(initializer.getMetaDataRepositoryLocation(), initializer.getMetaDataRepositoryDBname(), initializer.getMetaDataRepositoryUname(), initializer.getMetaDataRepositoryPass());
    
    this.currentHealthReportBuilder.setDerbyAlive(isCxnAlive);
    this.currentHealthReportBuilder.setElasticSearch(Boolean.valueOf(rollHealthReportEsIsAlive()));
    this.currentHealthReportBuilder.setEndTime(now - 1L);
    HealthRecord record = this.currentHealthReportBuilder.createHealthRecord();
    persistHealthRecordInES(record);
    if (Logger.getLogger("Monitor").isDebugEnabled()) {
      Logger.getLogger("Monitor").debug("Created health object: \n" + record.toString());
    }
    this.currentHealthCreateTime = now;
    this.currentHealthReportBuilder = new HealthRecordBuilder();
    this.currentHealthReportBuilder.setUuid(new UUID(now));
    this.currentHealthReportBuilder.setStartTime(now);
  }
  
  private MetaInfo.Initializer getInitializerObject()
  {
    Map<String, MetaInfo.Initializer> startUpMap = HazelcastSingleton.get().getMap("#startUpMap");
    String clusterName = HazelcastSingleton.getClusterName();
    return (MetaInfo.Initializer)startUpMap.get(clusterName);
  }
  
  private boolean rollHealthReportEsIsAlive()
  {
    String queryBetweenTwoTimestamps = String.format("{ \"select\": [ \"*\" ],\"from\":   [ \"%s\" ],\"where\":  { \"and\": [{ \"oper\": \"%s\", \"attr\": \"%s\", \"value\": %s },{ \"oper\": \"%s\", \"attr\": \"%s\", \"value\": %s }]}}", new Object[] { monitorEventDataType.getWActionStore().getName(), "gte", "timeStamp", String.valueOf(0), "lt", "timeStamp", String.valueOf(0) });
    
    String qu = queryBetweenTwoTimestamps;
    try
    {
      JsonNode jsonQuery = Utility.objectMapper.readTree(qu);
      WActionQuery q = monitorModelPersistedWActionStore.prepareQuery(jsonQuery);
      q.execute();
      
      return true;
    }
    catch (IOException e)
    {
      logger.warn("Unable to do a trivial ES query, indicating that ES is unavailable", e);
    }
    return false;
  }
  
  public static String renderCpuPercent(long nanosPerSec)
  {
    double cpuPercent = nanosPerSec / 1.0E9D * 100.0D;
    String cpuPercentString = String.format("%2.1f%%", new Object[] { Double.valueOf(cpuPercent) });
    return cpuPercentString;
  }
  
  public static String renderCpuPerNodePercent(long nanosPerSec, int nodes)
  {
    double cpuPercent = nanosPerSec / 1.0E9D / nodes * 100.0D;
    String cpuPercentString = String.format("%2.1f%%", new Object[] { Double.valueOf(cpuPercent) });
    return cpuPercentString;
  }
  
  public void run() {}
}
