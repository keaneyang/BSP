package com.bloom.persistence;

import com.bloom.cache.CacheConfiguration;
import com.bloom.cache.CacheManager;
import com.bloom.cache.CachingProvider;
import com.bloom.cache.Filter;
import com.bloom.cache.ICache;
import com.bloom.cache.CacheConfiguration.PartitionManagerType;
import com.bloom.classloading.BundleDefinition;
import com.bloom.classloading.WALoader;
import com.bloom.classloading.BundleDefinition.Type;
import com.bloom.distribution.WAIndex.FieldAccessor;
import com.bloom.distribution.WAQuery.ResultStats;
import com.bloom.gen.RTMappingGenerator;
import com.bloom.intf.PersistenceLayer;
import com.bloom.intf.PersistencePolicy;
import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.metaRepository.MDRepository;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.proc.events.JsonNodeEvent;
import com.bloom.runtime.BaseServer;
import com.bloom.runtime.Interval;
import com.bloom.runtime.Pair;
import com.bloom.runtime.PartitionManager;
import com.bloom.runtime.Server;
import com.bloom.runtime.channels.Channel;
import com.bloom.runtime.channels.SimpleChannel;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.components.FlowComponent;
import com.bloom.runtime.components.Flushable;
import com.bloom.runtime.components.PubSub;
import com.bloom.runtime.containers.TaskEvent;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Flow;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.Namespace;
import com.bloom.runtime.meta.MetaInfo.WActionStore;
import com.bloom.runtime.monitor.MonitorEventsCollection;
import com.bloom.runtime.utils.FieldToObject;
import com.bloom.security.Password;
import com.bloom.security.WASecurityManager;
import com.bloom.uuid.UUID;
import com.bloom.waction.Waction;
import com.bloom.waction.WactionContext;
import com.bloom.waction.WactionKey;
import com.bloom.waction.WactionListener;
import com.bloom.wactionstore.CheckpointManager;
import com.bloom.wactionstore.DataType;
import com.bloom.wactionstore.WAction;
import com.bloom.wactionstore.WActionStoreManager;
import com.bloom.wactionstore.WActionStores;
import com.bloom.wactionstore.exceptions.WActionStoreException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.bloom.event.SimpleEvent;
import com.bloom.recovery.Path;
import com.bloom.recovery.Position;
import com.bloom.recovery.SourcePosition;
import com.bloom.runtime.RecordKey;
import com.bloom.runtime.containers.ITaskEvent;
import com.bloom.runtime.containers.WAEvent;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javax.cache.Cache.Entry;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.persistence.spi.PersistenceUnitTransactionType;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class WactionStore
  extends FlowComponent
  implements WactionListener, PubSub, Flushable
{
  private CacheManager manager;
  
  class LRUNode
  {
    WactionKey wactionKey;
    WactionStore wactionStore;
    
    public LRUNode(WactionKey wactionKey, WactionStore wactionStore)
    {
      this.wactionKey = wactionKey;
      this.wactionStore = wactionStore;
    }
    
    public boolean equals(Object obj)
    {
      if (!(obj instanceof LRUNode)) {
        return false;
      }
      return this.wactionKey.equals(((LRUNode)obj).wactionKey);
    }
    
    public int hashCode()
    {
      return this.wactionKey.hashCode();
    }
  }
  
  public static enum TARGETDATABASE
  {
    UNKNOWN,  MONGODB,  ONDB;
    
    private TARGETDATABASE() {}
  }
  
  private static Logger logger = Logger.getLogger(WactionStore.class);
  public static final String MAX_PERSIST_QUEUE_SIZE = "MAX_PERSIST_QUEUE_SIZE";
  public static final String BATCH_WRITING_SIZE = "BATCH_WRITING_SIZE";
  public static final int DEFAULT_BATCH_WRITING_SIZE = 10000;
  public static final String MAX_THREAD_NUM = "MAX_THREAD_NUM";
  public static final int DEFAULT_MAX_THREAD_NUM = 10;
  public static final String DISABLE_MEMORY_CACHE = "DISABLE_MEMORY_CACHE";
  private static final ConcurrentMap<String, WactionStore> stores = new ConcurrentHashMap();
  private String pu_name;
  private String fullName;
  private String fullTableName;
  private String eventTableName;
  private String wactionContextClassName;
  private Class<?> wactionContextClass;
  private String wactionClassName;
  private Class<?> wactionClass;
  ICache wactions = null;
  ICache latestContexts;
  private PersistenceLayer persistenceLayer;
  private PersistencePolicy persistencePolicy;
  private static LRUList expungeableWactions;
  private static MemoryMonitor memoryMonitor;
  public MetaInfo.Type contextBeanDef;
  private final MetaInfo.WActionStore storeMetaInfo;
  private List<Pair<String, FieldFactory>> ctxKeyFacs;
  private List<Pair<String, FieldFactory>> ctxFieldFacs;
  private List<MetaInfo.Type> eventBeanDefs;
  private List<Class<?>> eventBeanClasses;
  private boolean persistenceAvailable = false;
  private ExecutorService executor;
  private boolean isNoSQL = false;
  private TARGETDATABASE targetDbType = TARGETDATABASE.UNKNOWN;
  private volatile long created = 0L;
  private Position waitPosition = null;
  private final Channel output;
  private boolean evictWactions = true;
  private boolean isRecoveryEnabled = false;
  public boolean isCrashed = false;
  public Exception crashCausingException = null;
  private boolean relaxSchemaCheck = false;
  
  public static void remove(UUID uuid)
  {
    WactionStore store = (WactionStore)stores.remove(uuid.getUUIDString());
    if (store != null) {
      store.wactions = null;
    }
  }
  
  public static WactionStore get(UUID uuid, BaseServer srv)
    throws Exception
  {
    MDRepository metaDataRepository = MetadataRepository.getINSTANCE();
    WactionStore store = null;
    synchronized (stores)
    {
      store = (WactionStore)stores.get(uuid.getUUIDString());
      if (store == null)
      {
        MetaInfo.WActionStore wsMetaInfo = (MetaInfo.WActionStore)metaDataRepository.getMetaObjectByUUID(uuid, WASecurityManager.TOKEN);
        if (wsMetaInfo == null) {
          return null;
        }
        store = new WactionStore(wsMetaInfo, srv);
        if (srv != null) {
          srv.putOpenObject(store);
        }
        store.init();
        stores.put(uuid.getUUIDString(), store);
      }
    }
    return store;
  }
  
  private WActionStore persistedWActionStore = null;
  private DataType persistedWActionType = null;
  private final Set<String> contextAttributes = new HashSet(0);
  private final Map<String, Set<String>> eventAttributes = new HashMap(0);
  private static final JsonNode wActionStoreMetadata = com.bloom.wactionstore.Utility.objectMapper.createArrayNode().add("id").add("any").add("timestamp").add("ttl").add("checkpoint");
  private static final Map<String, WActionDataType> javaTypeToWActionType = new HashMap();
  private static final String MSG_EXC_DATA_TYPE_CREATE_FAIL = "Unable to create data type '%s' in WActionStore '%s'";
  private static final String MSG_INF_CONTEXT_TYPE_FOUND = "Have context type '%s' for WActionStore '%s'";
  private static final String MSG_WRN_JAVA_TYPE_UNSUPPORTED = "Unsupported Java data type, '%s' for attribute '%s' in type '%s'";
  private static final String MSG_WRN_ES_SCHEMA_UNSUPPORTED = "Unsupported Java data type, '%s' for attribute '%s' in type '%s'. %s";
  private static final String MSG_WRN_WACTIONSTORE_DROP_FAILED = "Failed to drop WActionStore '%s'";
  
  static
  {
    javaTypeToWActionType.put("byte", new WActionDataType("integer", false));
    javaTypeToWActionType.put("short", new WActionDataType("integer", false));
    javaTypeToWActionType.put("int", new WActionDataType("integer", false));
    javaTypeToWActionType.put("java.lang.Byte", new WActionDataType("integer", true));
    javaTypeToWActionType.put("java.lang.Short", new WActionDataType("integer", true));
    javaTypeToWActionType.put("java.lang.Integer", new WActionDataType("integer", true));
    
    javaTypeToWActionType.put("long", new WActionDataType("long", false));
    javaTypeToWActionType.put("java.lang.Long", new WActionDataType("long", true));
    
    javaTypeToWActionType.put("float", new WActionDataType("double", false));
    javaTypeToWActionType.put("double", new WActionDataType("double", false));
    javaTypeToWActionType.put("java.lang.Float", new WActionDataType("double", true));
    javaTypeToWActionType.put("java.lang.Double", new WActionDataType("double", true));
    javaTypeToWActionType.put("java.lang.Number", new WActionDataType("double", true));
    
    javaTypeToWActionType.put("boolean", new WActionDataType("boolean", false));
    javaTypeToWActionType.put("java.lang.Boolean", new WActionDataType("boolean", true));
    
    javaTypeToWActionType.put("string", new WActionDataType("string", false));
    javaTypeToWActionType.put("java.lang.String", new WActionDataType("string", true));
    
    javaTypeToWActionType.put("datetime", new WActionDataType("datetime", false));
    javaTypeToWActionType.put("org.joda.time.DateTime", new WActionDataType("datetime", true));
  }
  
  public static void drop(MetaInfo.WActionStore wactionStore)
  {
    logger.info("Dropping wactionstore " + wactionStore.getName());
    Map<String, Object> properties = wactionStore.getProperties();
    if (usesNewWActionStore(properties))
    {
      String wActionStoreName = wactionStore.getFullName();
      WActionStoreManager manager = WActionStores.getInstance(properties);
      if (!manager.removeUsingAlias(wActionStoreName)) {
        logger.warn(String.format("Failed to drop WActionStore '%s'", new Object[] { wActionStoreName }));
      }
    }
  }
  
  private DataType getPersistentDataType(Map<String, Object> properties)
    throws ElasticSearchSchemaException
  {
    String wActionStoreName = this.fullName;
    String contextTypeName = this.contextBeanDef.nsName + ':' + this.contextBeanDef.name;
    JsonNode wActionStoreSchema = createWActionStoreSchema(properties);
    DataType result = getOrCreateWActionStoreType(this.persistedWActionStore, contextTypeName, wActionStoreSchema, this);
    logger.info(String.format("Have context type '%s' for WActionStore '%s'", new Object[] { contextTypeName, wActionStoreName }));
    return result;
  }
  
  private static WActionStore getOrCreateWActionStore(String wActionStoreName, Map<String, Object> properties)
  {
    WActionStoreManager manager = WActionStores.getInstance(properties);
    return manager.getOrCreate(wActionStoreName, properties);
  }
  
  private static DataType getOrCreateWActionStoreType(WActionStore wActionStore, String dataTypeName, JsonNode dataTypeSchema, WactionStore ws)
  {
    DataType contextType = wActionStore.setDataType(dataTypeName, dataTypeSchema, ws);
    if (contextType == null)
    {
      String wActionStoreName = wActionStore.getName();
      throw new WActionStoreException(String.format("Unable to create data type '%s' in WActionStore '%s'", new Object[] { dataTypeName, wActionStoreName }));
    }
    return contextType;
  }
  
  private JsonNode createWActionStoreSchema(Map<String, Object> properties)
    throws ElasticSearchSchemaException
  {
    ObjectNode result = com.bloom.wactionstore.Utility.objectMapper.createObjectNode();
    ArrayNode contextItems = createDataTypeSchema(this.contextBeanDef, this.contextAttributes, properties);
    JsonNode metadataItems = getMetadataArray();
    if (contextItems != null)
    {
      contextItems.add(makeFieldNode("$id", "string"));
      contextItems.add(makeFieldNode("$timestamp", "datetime"));
      contextItems.add(makeFieldNode("$node_name", "java.lang.String"));
      contextItems.add(makeFieldNode("$checkpoint", "java.lang.Long"));
      result.set("context", contextItems);
    }
    result.set("metadata", metadataItems);
    
    ArrayNode eventTypes = getEventTypes(this.eventBeanDefs, this.eventAttributes, properties);
    if (eventTypes != null) {
      result.set("events", eventTypes);
    }
    return result;
  }
  
  private JsonNode getMetadataArray()
  {
    return wActionStoreMetadata;
  }
  
  private ArrayNode getEventTypes(List<MetaInfo.Type> eventTypes, Map<String, Set<String>> eventAttributes, Map<String, Object> properties)
    throws ElasticSearchSchemaException
  {
    ArrayNode result = null;
    if ((eventTypes != null) && (!eventTypes.isEmpty()))
    {
      result = com.bloom.wactionstore.Utility.objectMapper.createArrayNode();
      for (MetaInfo.Type eventType : eventTypes)
      {
        ObjectNode eventSchema = com.bloom.wactionstore.Utility.objectMapper.createObjectNode();
        String eventTypeName = eventType.getFullName();
        String eventTypeSchemaName = eventType.nsName + ':' + eventType.name;
        Set<String> attributes = new HashSet();
        ArrayNode eventTypeSchema = createDataTypeSchema(eventType, attributes, properties);
        eventAttributes.put(eventTypeName, attributes);
        eventSchema.put("name", eventTypeSchemaName);
        eventSchema.set("type", eventTypeSchema);
        result.add(eventSchema);
      }
    }
    return result;
  }
  
  private ArrayNode createDataTypeSchema(MetaInfo.Type type, Set<String> attributes, Map<String, Object> properties)
    throws ElasticSearchSchemaException
  {
    ArrayNode dataTypeSchema = com.bloom.wactionstore.Utility.objectMapper.createArrayNode();
    for (Map.Entry<String, String> field : type.fields.entrySet())
    {
      String fieldName = (String)field.getKey();
      String fieldType = (String)field.getValue();
      JsonNode fieldNode = makeFieldNode(fieldName, fieldType);
      if (fieldNode != null)
      {
        dataTypeSchema.add(fieldNode);
        attributes.add(fieldName);
      }
      else if ((properties != null) && (properties.containsKey("elasticsearch.relax_schema")))
      {
        String value = (String)properties.get("elasticsearch.relax_schema");
        if (value.equalsIgnoreCase("true"))
        {
          this.relaxSchemaCheck = true;
        }
        else if (value.equalsIgnoreCase("false"))
        {
          String typeName = type.getFullName();
          logger.error(String.format("Unsupported Java data type, '%s' for attribute '%s' in type '%s'", new Object[] { fieldType, fieldName, typeName }));
          throw new ElasticSearchSchemaException(String.format("Unsupported Java data type, '%s' for attribute '%s' in type '%s'", new Object[] { fieldType, fieldName, typeName }));
        }
      }
      else
      {
        String typeName = type.getFullName();
        String message = "You might want to relax the schema by setting the elasticsearch.relax_schema to true while creating waction store. This will insert the data into elastic search, but your query through Striim client might fail";
        logger.error(String.format("Unsupported Java data type, '%s' for attribute '%s' in type '%s'. %s", new Object[] { fieldType, fieldName, typeName, message }));
        throw new ElasticSearchSchemaException(String.format("Unsupported Java data type, '%s' for attribute '%s' in type '%s'. %s", new Object[] { fieldType, fieldName, typeName, message }));
      }
    }
    return dataTypeSchema;
  }
  
  private static JsonNode makeFieldNode(String name, String javaTypeName)
  {
    ObjectNode result = null;
    WActionDataType wActionDataType = (WActionDataType)javaTypeToWActionType.get(javaTypeName);
    if (wActionDataType != null)
    {
      result = com.bloom.wactionstore.Utility.objectMapper.createObjectNode();
      result.put("name", name);
      result.put("type", wActionDataType.typeName);
      result.put("nullable", wActionDataType.nullable);
    }
    return result;
  }
  
  private static class WActionDataType
  {
    public final String typeName;
    public final boolean nullable;
    
    WActionDataType(String typeName, boolean nullable)
    {
      this.typeName = typeName;
      this.nullable = nullable;
    }
    
    public boolean equals(Object obj)
    {
      if ((obj instanceof WActionDataType))
      {
        WActionDataType instance = (WActionDataType)WActionDataType.class.cast(obj);
        return (instance.typeName.equals(this.typeName)) && (instance.nullable == this.nullable);
      }
      return false;
    }
    
    public int hashCode()
    {
      return this.typeName.hashCode() ^ (this.nullable ? 1 : 0);
    }
  }
  
  private WAction addToWActionStore(Waction waction)
  {
    if (this.persistedWActionType != null)
    {
      WActionStoreManager manager = this.persistedWActionStore.getManager();
      WAction newWAction = convertOldWActionToNew(waction);
      if (this.isRecoveryEnabled)
      {
        CheckpointManager checkpointManager = manager.getCheckpointManager();
        synchronized (checkpointManager)
        {
          checkpointManager.add(this.fullName, newWAction, waction.getPosition());
        }
      }
      this.persistedWActionType.queue(newWAction);
      return newWAction;
    }
    return null;
  }
  
  private WAction convertOldWActionToNew(Waction waction)
  {
    ObjectNode wActionJson = (ObjectNode)com.bloom.wactionstore.Utility.objectMapper.valueToTree(waction);
    if (!this.relaxSchemaCheck)
    {
      wActionJson.retain(this.contextAttributes);
    }
    else
    {
      String[] fields = { "_id", "id", "internalWactionStoreName", "uuid", "wactionTs", "key", "mapKey", "eventsNotAccessedDirectly", "wactionStatus", "position", "notificationFlag" };
      List<String> fieldsToRemove = java.util.Arrays.asList(fields);
      wActionJson.remove(fieldsToRemove);
    }
    setMissingValuesToNull(this.contextAttributes, wActionJson);
    
    WAction wAction = new WAction();
    wAction.setAll(wActionJson);
    wAction.put("$id", waction.id);
    wAction.put("$timestamp", waction.wactionTs);
    
    addNewWActionEvents(wAction, waction.getEvents());
    
    return wAction;
  }
  
  private void addNewWActionEvents(WAction wAction, List<SimpleEvent> events)
  {
    if ((events != null) && (!events.isEmpty())) {
      for (SimpleEvent event : events) {
        addNewWActionEvent(wAction, event);
      }
    }
  }
  
  private void addNewWActionEvent(WAction wAction, SimpleEvent event)
  {
    String eventClassName = event.getClass().getName();
    MetaInfo.Type eventBeanDef = getEventBeanDef(eventClassName);
    if (eventBeanDef != null)
    {
      String eventTypeName = eventBeanDef.getFullName();
      ObjectNode eventJson = (ObjectNode)com.bloom.wactionstore.Utility.objectMapper.valueToTree(event);
      Set<String> attributes = (Set)this.eventAttributes.get(eventTypeName);
      eventJson.retain(attributes);
      setMissingValuesToNull(attributes, eventJson);
      
      ArrayNode eventList = getEventList(wAction, eventBeanDef);
      eventList.add(eventJson);
    }
  }
  
  private void setMissingValuesToNull(Set<String> attributes, ObjectNode eventJson)
  {
    for (String attribute : attributes) {
      if (!eventJson.has(attribute)) {
        eventJson.putNull(attribute);
      }
    }
  }
  
  private MetaInfo.Type getEventBeanDef(String eventClassName)
  {
    MetaInfo.Type result = null;
    for (MetaInfo.Type eventBeanDef : this.eventBeanDefs) {
      if (eventClassName.equals(eventBeanDef.className))
      {
        result = eventBeanDef;
        break;
      }
    }
    return result;
  }
  
  private static ArrayNode getEventList(WAction wAction, MetaInfo.Type eventBeanDef)
  {
    String eventTypeName = eventBeanDef.nsName + ':' + eventBeanDef.name;
    ArrayNode eventList = (ArrayNode)wAction.get(eventTypeName);
    if (eventList == null)
    {
      eventList = com.bloom.wactionstore.Utility.objectMapper.createArrayNode();
      wAction.set(eventTypeName, eventList);
    }
    return eventList;
  }
  
  public static WactionStore get(String fullName)
    throws Exception
  {
    MDRepository metadataRepository = MetadataRepository.getINSTANCE();
    String namespace = fullName.split("\\.")[0];
    String name = fullName.split("\\.")[1];
    MetaInfo.WActionStore wsInfo = (MetaInfo.WActionStore)metadataRepository.getMetaObjectByName(EntityType.WACTIONSTORE, namespace, name, null, WASecurityManager.TOKEN);
    if (wsInfo != null) {
      return get(wsInfo.uuid, null);
    }
    throw new RuntimeException("Waction store " + fullName + " does not exist, please ensure the application name is included as appname.storename");
  }
  
  public boolean usesInMemoryWActionStore()
  {
    if (this.storeMetaInfo.properties.containsKey("DISABLE_MEMORY_CACHE")) {
      return "true".equalsIgnoreCase(this.storeMetaInfo.properties.get("DISABLE_MEMORY_CACHE").toString());
    }
    Type type = this.storeMetaInfo.wactionstoretype;
    return (type == Type.IN_MEMORY) || (type == Type.INTERVAL);
  }
  
  public boolean usesOldWActionStore()
  {
    return this.storeMetaInfo.usesIntervalBasedWActionStore();
  }
  
  public boolean usesNewWActionStore()
  {
    Type type = this.storeMetaInfo.wactionstoretype;
    return type == Type.STANDARD;
  }
  
  public static boolean usesNewWActionStore(Map<String, Object> properties)
  {
    Type type = Type.getType(properties, null);
    return type == Type.STANDARD;
  }
  
  private Map<String, Object> extractDBProps(Map<String, Object> storeProperties)
  {
    if (logger.isInfoEnabled()) {
      logger.info("properties for WactionStore persistence :\n" + storeProperties);
    }
    Map<String, Object> storePropCopy = new HashMap();
    for (String key : storeProperties.keySet()) {
      storePropCopy.put(key.toUpperCase(), storeProperties.get(key));
    }
    Map<String, Object> props = new HashMap();
    props.putAll(storePropCopy);
    props.put("javax.persistence.transactionType", PersistenceUnitTransactionType.RESOURCE_LOCAL.name());
    props.put("eclipselink.classloader", WALoader.getDelegate());
    props.put("eclipselink.logging.level", (String)storePropCopy.get("LOGGING_LEVEL") == null ? "SEVERE" : (String)storePropCopy.get("LOGGING_LEVEL"));
    props.put("eclipselink.weaving.changetracking", "false");
    props.put("eclipselink.weaving", "false");
    props.put("eclipselink.cache.shared.default", "false");
    
    props.put("eclipselink.cache.shared.", "false");
    props.put("eclipselink.cache.type.default", "NONE");
    props.put("eclipselink.cache.type.", "NONE");
    props.put("eclipselink.persistence-context.reference-mode", "WEAK");
    
    String MONGODB = "org.eclipse.persistence.nosql.adapters.mongo.MongoPlatform";
    String ONDB = "org.eclipse.persistence.nosql.adapters.nosql.OracleNoSQLPlatform";
    if ((props.get("CONTEXT_TABLE") == null) || (((String)props.get("CONTEXT_TABLE")).isEmpty()))
    {
      if (logger.isDebugEnabled()) {
        logger.debug("No context tablename provided by user. Using system generated tablename:" + this.fullTableName);
      }
    }
    else
    {
      this.fullTableName = ((String)props.get("CONTEXT_TABLE"));
      if (logger.isInfoEnabled()) {
        logger.info("Using context tablename:" + this.fullTableName);
      }
    }
    if ((props.get("EVENT_TABLE") == null) || (((String)props.get("EVENT_TABLE")).isEmpty()))
    {
      if (logger.isDebugEnabled()) {
        logger.debug("No event tablename provided by user. Will use system generated name.");
      }
    }
    else
    {
      this.eventTableName = ((String)props.get("EVENT_TABLE"));
      if (logger.isInfoEnabled()) {
        logger.info("Using event tablename:" + this.eventTableName);
      }
    }
    if (storePropCopy.get("NOSQL_PROPERTY") == null)
    {
      if (logger.isInfoEnabled()) {
        logger.info("RDBMS is used for persisting, setting properties ");
      }
      props.put("javax.persistence.jdbc.driver", (String)storePropCopy.get("JDBC_DRIVER"));
      props.put("javax.persistence.jdbc.url", (String)storePropCopy.get("JDBC_URL"));
      props.put("javax.persistence.jdbc.user", (String)storePropCopy.get("JDBC_USER"));
      props.put("javax.persistence.jdbc.password", Password.getPlainStatic((String)storePropCopy.get("JDBC_PASSWORD")));
      props.put("eclipselink.ddl-generation", (String)storePropCopy.get("DDL_GENERATION"));
      props.put("eclipselink.jdbc.batch-writing", "JDBC");
      if (props.get("BATCH_WRITING_SIZE") != null) {
        try
        {
          com.bloom.wactionstore.Utility.extractInt(props.get("BATCH_WRITING_SIZE"));
          props.put("eclipselink.jdbc.batch-writing.size", props.get("BATCH_WRITING_SIZE").toString());
        }
        catch (NumberFormatException nfe)
        {
          props.put("eclipselink.jdbc.batch-writing.size", String.valueOf(10000));
        }
      }
    }
    else
    {
      this.isNoSQL = true;
      if (logger.isInfoEnabled()) {
        logger.info("NoSQL is used for persisting, setting properties ");
      }
      String targetDatabase = (String)storePropCopy.get("TARGET_DATABASE");
      if ((targetDatabase == null) || (targetDatabase.isEmpty()))
      {
        if (logger.isInfoEnabled()) {
          logger.info("no target supplied. assuming this is mongodb");
        }
        props.put("eclipselink.target-database", "org.eclipse.persistence.nosql.adapters.mongo.MongoPlatform");
        props.put("eclipselink.nosql.property.", (String)storePropCopy.get("NOSQL_PROPERTY"));
        props.put("eclipselink.nosql.connection-spec", "org.eclipse.persistence.nosql.adapters.mongo.MongoConnectionSpec");
        props.put("eclipselink.nosql.property.mongo.db", (String)storePropCopy.get("DB_NAME"));
        props.put("eclipselink.nosql.property.mongo.host", (String)storePropCopy.get("NOSQL_PROPERTY"));
        this.targetDbType = TARGETDATABASE.MONGODB;
      }
      else if ((targetDatabase.equals("org.eclipse.persistence.nosql.adapters.mongo.MongoPlatform")) || (targetDatabase.equalsIgnoreCase("mongo")) || (targetDatabase.equalsIgnoreCase("mongodb")))
      {
        if (logger.isInfoEnabled()) {
          logger.info("this is mongodb");
        }
        props.put("eclipselink.target-database", "org.eclipse.persistence.nosql.adapters.mongo.MongoPlatform");
        props.put("eclipselink.nosql.property.", (String)storePropCopy.get("NOSQL_PROPERTY"));
        props.put("eclipselink.nosql.connection-spec", "org.eclipse.persistence.nosql.adapters.mongo.MongoConnectionSpec");
        props.put("eclipselink.nosql.property.mongo.db", (String)storePropCopy.get("DB_NAME"));
        props.put("eclipselink.nosql.property.mongo.host", (String)storePropCopy.get("NOSQL_PROPERTY"));
        this.targetDbType = TARGETDATABASE.MONGODB;
      }
      else if ((targetDatabase.equals("org.eclipse.persistence.nosql.adapters.nosql.OracleNoSQLPlatform")) || (targetDatabase.equalsIgnoreCase("ondb")) || (targetDatabase.equalsIgnoreCase("oraclenosql")))
      {
        if (logger.isInfoEnabled()) {
          logger.info("this is ondb");
        }
        props.put("eclipselink.target-database", "org.eclipse.persistence.nosql.adapters.nosql.OracleNoSQLPlatform");
        props.put("eclipselink.nosql.property.", (String)storePropCopy.get("NOSQL_PROPERTY"));
        props.put("eclipselink.nosql.connection-spec", "org.eclipse.persistence.nosql.adapters.nosql.OracleNoSQLConnectionSpec");
        props.put("eclipselink.nosql.property.nosql.store", (String)storePropCopy.get("DB_NAME"));
        props.put("eclipselink.nosql.property.nosql.host", (String)storePropCopy.get("NOSQL_PROPERTY"));
        this.targetDbType = TARGETDATABASE.ONDB;
      }
      else
      {
        if (logger.isInfoEnabled()) {
          logger.info("unknown target supplied. assuming this is mongodb");
        }
        props.put("eclipselink.target-database", "org.eclipse.persistence.nosql.adapters.mongo.MongoPlatform");
        props.put("eclipselink.nosql.property.", (String)storePropCopy.get("NOSQL_PROPERTY"));
        props.put("eclipselink.nosql.connection-spec", "org.eclipse.persistence.nosql.adapters.mongo.MongoConnectionSpec");
        props.put("eclipselink.nosql.property.mongo.db", (String)storePropCopy.get("DB_NAME"));
        props.put("eclipselink.nosql.property.mongo.host", (String)storePropCopy.get("NOSQL_PROPERTY"));
        this.targetDbType = TARGETDATABASE.MONGODB;
      }
    }
    return props;
  }
  
  private WactionStore(MetaInfo.WActionStore storeMetaInfo, BaseServer srv)
  {
    super(srv, storeMetaInfo);
    this.storeMetaInfo = storeMetaInfo;
    this.output = (srv == null ? new SimpleChannel() : srv.createChannel(this));
  }
  
  private void init()
    throws MetaDataRepositoryException, Exception
  {
    MetadataRepository metadataRepository = MetadataRepository.getINSTANCE();
    String appName = getMetaNsName();
    if (appName == null)
    {
      MetaInfo.Namespace app = (MetaInfo.Namespace)metadataRepository.getMetaObjectByUUID(this.storeMetaInfo.namespaceId, WASecurityManager.TOKEN);
      if (app != null) {
        appName = app.name;
      }
    }
    this.fullName = (appName + "." + getMetaName());
    
    this.fullTableName = (appName + "_" + getMetaName());
    this.pu_name = this.fullTableName;
    
    Level logLevel = Level.DEBUG;
    String propertyFormat;
    String propertyFormatNull;
    if (logger.isEnabledFor(logLevel))
    {
      logger.log(logLevel, String.format("Initializing WActionStore '%s'", new Object[] { this.fullName }));
      logger.log(logLevel, String.format("  Type:       %s", new Object[] { this.storeMetaInfo.type.name() }));
      logger.log(logLevel, String.format("  Old:        %s", new Object[] { Boolean.valueOf(usesOldWActionStore()) }));
      logger.log(logLevel, String.format("  New:        %s", new Object[] { Boolean.valueOf(usesNewWActionStore()) }));
      logger.log(logLevel, String.format("  In-Memory:  %s", new Object[] { Boolean.valueOf(usesInMemoryWActionStore()) }));
      logger.log(logLevel, String.format("  Properties:", new Object[0]));
      
      int nameLength = 0;
      for (String propertyName : this.storeMetaInfo.properties.keySet()) {
        nameLength = Math.max(nameLength, propertyName.length());
      }
      propertyFormat = "    %-" + nameLength + "s : '%s'";
      propertyFormatNull = "    %-" + nameLength + "s : null";
      for (Map.Entry<String, Object> property : this.storeMetaInfo.properties.entrySet())
      {
        String propertyName = (String)property.getKey();
        Object propertyValue = property.getValue();
        if (propertyValue != null) {
          logger.log(logLevel, String.format(propertyFormat, new Object[] { propertyName, propertyValue.toString() }));
        } else {
          logger.log(logLevel, String.format(propertyFormatNull, new Object[] { propertyName }));
        }
      }
    }
    if (usesInMemoryWActionStore())
    {
      CachingProvider provider = (CachingProvider)Caching.getCachingProvider(CachingProvider.class.getName());
      this.manager = ((CacheManager)provider.getCacheManager());
      int numReplicas = CacheConfiguration.getNumReplicasFromProps(this.storeMetaInfo.getProperties());
      
      PartitionManager.registerCache(this.fullName, this.storeMetaInfo.uuid);
      MutableConfiguration<WactionKey, Waction> wactionsConfig = new CacheConfiguration(numReplicas, CacheConfiguration.PartitionManagerType.SIMPLE);
      wactionsConfig.setStoreByValue(false);
      wactionsConfig.setStatisticsEnabled(false);
      
      this.wactions = ((ICache)this.manager.createCache(this.fullName, wactionsConfig));
      
      this.wactions.addIndex("key", WAIndex.Type.hash, new WAIndex.FieldAccessor()
      {
        public Object getField(Waction parent)
        {
          return parent.key;
        }
      });
      this.wactions.addIndex("ts", WAIndex.Type.tree, new WAIndex.FieldAccessor()
      {
        public Long getField(Waction parent)
        {
          return Long.valueOf(parent.wactionTs);
        }
      });
      PartitionManager.registerCache(getCacheName(), this.storeMetaInfo.uuid);
      MutableConfiguration<Object, WactionContext> latestContextsConfig = new CacheConfiguration(numReplicas, CacheConfiguration.PartitionManagerType.SIMPLE);
      this.latestContexts = ((ICache)this.manager.createCache(getCacheName(), latestContextsConfig));
    }
    if (HazelcastSingleton.isClientMember()) {
      this.persistenceLayer = null;
    } else {
      createPersistingPolicy();
    }
    this.contextBeanDef = ((MetaInfo.Type)metadataRepository.getMetaObjectByUUID(this.storeMetaInfo.contextType, WASecurityManager.TOKEN));
    this.wactionContextClassName = (this.contextBeanDef.className + "_wactionContext");
    this.wactionClassName = ("wa.Waction_" + this.pu_name);
    try
    {
      this.wactionContextClass = WALoader.get().loadClass(this.wactionContextClassName);
    }
    catch (Exception e)
    {
      logger.error("Problem loading waction context class for " + this.fullName, e);
    }
    try
    {
      this.wactionClass = WALoader.get().loadClass(this.wactionClassName);
      if (logger.isDebugEnabled()) {
        logger.debug("waction run time class loaded :" + this.wactionClassName);
      }
    }
    catch (Exception e)
    {
      logger.error("Problem loading waction class for " + this.fullName, e);
    }
    if ((usesOldWActionStore()) && (this.storeMetaInfo.frequency != null)) {
      createORMXmlDoc();
    }
    WALoader wal = WALoader.get();
    String bundleUri = wal.createIfNotExistsBundleDefinition(appName, BundleDefinition.Type.fieldFactory, getMetaName());
    BundleDefinition bundleDefinition = WALoader.get().getBundleDefinition(bundleUri);
    
    this.ctxKeyFacs = new ArrayList();
    this.ctxFieldFacs = new ArrayList();
    List<String> classNames = bundleDefinition.getClassNames();
    for (String cName : classNames) {
      try
      {
        Class cc = WALoader.get().loadClass(cName);
        FieldFactory waf = (FieldFactory)cc.newInstance();
        String fName = waf.getFieldName();
        if (this.contextBeanDef.keyFields.contains(fName)) {
          this.ctxKeyFacs.add(Pair.make(fName, waf));
        }
        this.ctxFieldFacs.add(Pair.make(fName, waf));
      }
      catch (ClassNotFoundException|InstantiationException|IllegalAccessException e)
      {
        logger.error("Problem creating field factory for " + this.contextBeanDef.getFullName() + " with error : " + e.getMessage());
      }
    }
    if ((this.storeMetaInfo.eventTypes != null) && (!this.storeMetaInfo.eventTypes.isEmpty()))
    {
      this.eventBeanDefs = new ArrayList();
      this.eventBeanClasses = new ArrayList();
      for (UUID typeUUID : this.storeMetaInfo.eventTypes)
      {
        MetaInfo.Type eventBeanDef = (MetaInfo.Type)metadataRepository.getMetaObjectByUUID(typeUUID, WASecurityManager.TOKEN);
        this.eventBeanDefs.add(eventBeanDef);
        try
        {
          Class<?> eventBeanClass = WALoader.get().loadClass(eventBeanDef.className);
          this.eventBeanClasses.add(eventBeanClass);
        }
        catch (ClassNotFoundException e)
        {
          logger.error("Problem loading event class " + eventBeanDef.className, e);
        }
      }
    }
    if ((usesOldWActionStore()) && (this.persistenceLayer != null)) {
      this.persistenceLayer.init(this.pu_name);
    }
    if (usesNewWActionStore())
    {
      this.persistedWActionStore = getOrCreateWActionStore(this.fullName, this.storeMetaInfo.properties);
      this.persistedWActionType = getPersistentDataType(this.storeMetaInfo.properties);
    }
    this.isRecoveryEnabled = (getCurrentApp(this.storeMetaInfo).getRecoveryType() != 0);
    logger.info("Recovery is " + (this.isRecoveryEnabled ? "enabled" : "disabled") + " for WActionStore '" + this.fullName + "'");
  }
  
  private void createPersistingPolicy()
  {
    Object evictionPolicy = this.storeMetaInfo.properties.get("evict");
    if (evictionPolicy != null) {
      this.evictWactions = (!"FALSE".equalsIgnoreCase(evictionPolicy.toString()));
    }
    if (logger.isDebugEnabled()) {
      logger.debug(this.fullName + ": evict wactions=" + this.evictWactions);
    }
    if ((this.storeMetaInfo.frequency != null) && (usesOldWActionStore()))
    {
      this.executor = (srv() == null ? null : srv().getThreadPool());
      this.persistenceAvailable = true;
      
      Map<String, Object> props = extractDBProps(this.storeMetaInfo.properties);
      props.put("wsname", getMetaName());
      
      String target_database = (String)this.storeMetaInfo.properties.get("eclipselink.target-database");
      if (target_database == null) {
        target_database = "rdbms";
      }
      if (this.targetDbType.equals(TARGETDATABASE.ONDB)) {
        target_database = "ONDB";
      }
      this.persistenceLayer = PersistenceFactory.createPersistenceLayer(target_database, PersistenceFactory.PersistingPurpose.WACTIONSTORE, this.pu_name, props);
      this.persistenceLayer.setStoreName(this.pu_name, this.fullTableName);
      this.persistencePolicy = PersistenceFactory.createPersistencePolicy(Long.valueOf(this.storeMetaInfo.frequency.value), this.persistenceLayer, srv(), props, this);
    }
    else
    {
      if (logger.isInfoEnabled()) {
        logger.info("persistence policy is none. so wactions will NOT be persisted. ");
      }
      this.persistenceLayer = null;
    }
  }
  
  private void createORMXmlDoc()
    throws MetaDataRepositoryException
  {
    String xml = "";
    String driver = (String)this.storeMetaInfo.properties.get("JDBC_URL");
    if (driver != null)
    {
      driver = driver.toLowerCase();
      if (driver.contains("sqlmxdriver"))
      {
        xml = RTMappingGenerator.createHibernateMappings(this.wactionClassName, this.contextBeanDef);
        RTMappingGenerator.addMappings(this.pu_name, xml);
        return;
      }
    }
    if (this.isNoSQL)
    {
      if (this.targetDbType.equals(TARGETDATABASE.ONDB)) {
        xml = RTMappingGenerator.createOrmMappingforRTWactionNoSQL(this.wactionClassName, this.fullTableName, this.contextBeanDef, TARGETDATABASE.ONDB);
      } else {
        xml = RTMappingGenerator.createOrmMappingforRTWactionNoSQL(this.wactionClassName, this.fullTableName, this.contextBeanDef, TARGETDATABASE.MONGODB);
      }
    }
    else if ((this.storeMetaInfo.eventTypes != null) && (this.storeMetaInfo.eventTypes.size() == 1))
    {
      MDRepository MDRepository = MetadataRepository.getINSTANCE();
      MetaInfo.Type eventType = (MetaInfo.Type)MDRepository.getMetaObjectByUUID((UUID)this.storeMetaInfo.eventTypes.get(0), WASecurityManager.TOKEN);
      xml = RTMappingGenerator.createOrmMappingforRTWactionRDBMSNew(this.wactionClassName, this.fullTableName, this.contextBeanDef.fields, eventType, this.eventTableName, this.storeMetaInfo.properties);
    }
    else
    {
      xml = RTMappingGenerator.createOrmMappingforRTWactionRDBMS(this.wactionClassName, this.fullTableName, this.contextBeanDef, this.eventTableName, this.storeMetaInfo.properties);
    }
    RTMappingGenerator.addMappings(this.pu_name, xml);
  }
  
  public Iterable<Waction> DBSearch(Object key, WactionQuery query)
  {
    Iterable<Waction> wactionsFromDB = null;
    Set<WactionKey> keys = new HashSet();
    if (this.persistenceLayer != null)
    {
      long T1 = System.nanoTime();
      if ((this.persistenceLayer instanceof HibernatePersistenceLayerImpl))
      {
        if (logger.isInfoEnabled()) {
          logger.info("this is hibernate query.");
        }
        ((HibernatePersistenceLayerImpl)this.persistenceLayer).init(this.fullTableName);
        Class wclazz = this.wactionClass;
        if ((key instanceof WactionKey)) {
          wactionsFromDB = ((HibernatePersistenceLayerImpl)this.persistenceLayer).getResults(wclazz, ((WactionKey)key).getWactionKeyStr(), query.getFilterMap());
        } else {
          wactionsFromDB = ((HibernatePersistenceLayerImpl)this.persistenceLayer).getResults(wclazz, query.getFilterMap(), keys);
        }
      }
      else
      {
        this.persistenceLayer.init(this.fullTableName);
        Class wclazz = this.wactionClass;
        if ((key instanceof WactionKey))
        {
          if (this.targetDbType.equals(TARGETDATABASE.ONDB)) {
            wactionsFromDB = this.persistenceLayer.getResults(wclazz, ((WactionKey)key).getWactionKeyStr(), query.getFilterMap());
          } else {
            wactionsFromDB = ((WactionStorePersistenceLayerImpl)this.persistenceLayer).getResults(wclazz, ((WactionKey)key).getWactionKeyStr(), query);
          }
        }
        else if (this.targetDbType.equals(TARGETDATABASE.ONDB)) {
          wactionsFromDB = ((ONDBPersistenceLayerImpl)this.persistenceLayer).getResults(wclazz, query.getFilterMap(), keys);
        } else {
          wactionsFromDB = ((WactionStorePersistenceLayerImpl)this.persistenceLayer).getResults(wclazz, query);
        }
      }
      long T2 = System.nanoTime();
      long diff = T2 - T1;
      if (logger.isInfoEnabled()) {
        logger.info("Executed DB query " + query + " in " + diff / 1000000.0D + "ms");
      }
    }
    return wactionsFromDB;
  }
  
  public static Object[] getWactionStoreDef(String storename)
    throws Exception
  {
    WactionStore ws = get(storename);
    Object[] defs = new Object[2];
    defs[0] = ws.contextBeanDef;
    defs[1] = ws.eventBeanDefs;
    return defs;
  }
  
  public static class WactionFilter
    implements Filter<WactionKey, Waction>, Serializable
  {
    private static final long serialVersionUID = 1368964098470210574L;
    public WactionQuery q;
    
    public WactionFilter(WactionQuery q)
    {
      this.q = q;
    }
    
    public boolean matches(WactionKey key, Waction value)
    {
      return this.q.matches(value);
    }
    
    public String toString()
    {
      return this.q.toString();
    }
  }
  
  public Collection<Waction> getWactions(long startTime, long endTime)
  {
    try
    {
      final WactionQuery q = new WactionQuery(this.fullName, startTime, endTime);
      Map<UUID, Waction> res = new LinkedHashMap();
      Future<Iterable<Waction>> dbFuture = null;
      if (this.persistenceAvailable) {
        dbFuture = this.executor.submit(new Callable()
        {
          public Iterable<Waction> call()
            throws Exception
          {
            return WactionStore.this.DBSearch(null, q);
          }
        });
      }
      Iterator<Cache.Entry<WactionKey, Waction>> it = this.wactions.iterator(new WactionFilter(q));
      while (it.hasNext())
      {
        Waction wa = (Waction)((Cache.Entry)it.next()).getValue();
        res.put(wa.uuid, wa);
      }
      if (this.persistenceAvailable) {
        try
        {
          Iterable<Waction> dbWactions = (Iterable)dbFuture.get();
          for (Waction wa : dbWactions) {
            if ((q.matches(wa)) && (!res.containsKey(wa.uuid))) {
              res.put(wa.uuid, wa);
            }
          }
          if ((dbWactions instanceof JPAIterable)) {
            ((JPAIterable)dbWactions).close();
          }
        }
        catch (InterruptedException|ExecutionException e)
        {
          logger.error("Problem obtaining wactions from db", e);
        }
      }
      return res.values();
    }
    catch (Throwable t)
    {
      logger.error("Problem executing waction query using", t);
      throw t;
    }
  }
  
  public Map<WactionKey, Map<String, Object>> getWactions(final WactionKey key, String[] fields, Map<String, Object> filter)
  {
    try
    {
      long stime = System.nanoTime();
      final WactionQuery q = new WactionQuery(this.fullName, this.contextBeanDef, fields, filter);
      q.setSingleKey(key);
      if (logger.isInfoEnabled()) {
        logger.info("Starting execution of query " + q);
      }
      if (!this.persistenceAvailable)
      {
        if (q.requiresResultStats())
        {
          WAQuery.ResultStats stats = this.wactions.queryStats(q);
          q.setGlobalStats(stats);
        }
        if ((usesInMemoryWActionStore()) && (q.getLatestPerKey) && (q.endTime == null) && (!q.getEvents))
        {
          long T1 = System.nanoTime();
          int count = 0;
          for (Object akey : this.latestContexts.getAllKeys())
          {
            Waction w = new Waction(akey);
            WactionContext wc = (WactionContext)this.latestContexts.get(akey);
            w.setContext(wc);
            
            q.runOne(w);
            count++;
          }
          long T2 = System.nanoTime();
          long diff = T2 - T1;
          if (logger.isInfoEnabled()) {
            logger.info("Executed latest context query " + q + " for " + count + " wactions in " + diff / 1000000.0D + "ms");
          }
        }
        else
        {
          long T1 = System.nanoTime();
          this.wactions.query(q);
          long T2 = System.nanoTime();
          long diff = T2 - T1;
          if (logger.isInfoEnabled()) {
            logger.info("Executed in-memory query " + q + " for " + q.processed + " wactions in " + diff / 1000000.0D + "ms");
          }
        }
      }
      else
      {
        Future<Iterable<Waction>> dbFuture = null;
        Iterable<Waction> dbWactions = null;
        boolean triedToGetDbWactions = false;
        
        Callable<Iterable<Waction>> dbTask = new Callable()
        {
          public Iterable<Waction> call()
            throws Exception
          {
            return WactionStore.this.DBSearch(key, q);
          }
        };
        dbFuture = this.executor.submit(dbTask);
        if (q.requiresResultStats())
        {
          WAQuery.ResultStats stats = this.wactions.queryStats(q);
          try
          {
            dbWactions = (Iterable)dbFuture.get();
          }
          catch (InterruptedException|ExecutionException e)
          {
            logger.error("Problem obtaining wactions from db", e);
          }
          triedToGetDbWactions = true;
          if (dbWactions != null)
          {
            WAQuery.ResultStats dbStats = q.getResultStats(dbWactions);
            if ((stats.startTime == 0L) || (dbStats.startTime < stats.startTime)) {
              stats.startTime = dbStats.startTime;
            }
            if ((stats.endTime == 0L) || (dbStats.endTime > stats.endTime)) {
              stats.endTime = dbStats.endTime;
            }
            stats.count += dbStats.count;
          }
          q.setGlobalStats(stats);
        }
        if ((usesInMemoryWActionStore()) && (q.getLatestPerKey) && (q.endTime == null) && (!q.getEvents))
        {
          if (logger.isDebugEnabled()) {
            logger.debug(this.fullName + ": Using only latest waction context");
          }
          long T1 = System.nanoTime();
          int count = 0;
          for (Object akey : this.latestContexts.getAllKeys())
          {
            Waction w = new Waction(akey);
            WactionContext wc = (WactionContext)this.latestContexts.get(akey);
            w.setContext(wc);
            
            q.runOne(w);
            count++;
          }
          long T2 = System.nanoTime();
          long diff = T2 - T1;
          if (logger.isInfoEnabled()) {
            logger.info("Executed latest context query " + q + " for " + count + " wactions in " + diff / 1000000.0D + "ms");
          }
        }
        else
        {
          long T1 = System.nanoTime();
          this.wactions.query(q);
          long T2 = System.nanoTime();
          long diff = T2 - T1;
          if (logger.isInfoEnabled()) {
            logger.info("Executed in-memory query " + q + " for " + q.processed + " wactions in " + diff / 1000000.0D + "ms");
          }
        }
        if (!triedToGetDbWactions) {
          try
          {
            dbWactions = (Iterable)dbFuture.get();
          }
          catch (InterruptedException|ExecutionException e)
          {
            logger.error("Problem obtaining wactions from db", e);
          }
        }
        if (dbWactions != null)
        {
          long T1 = System.nanoTime();
          int count = 0;
          for (Waction waction : dbWactions)
          {
            q.runOne(waction);
            count++;
          }
          long T2 = System.nanoTime();
          long diff = T2 - T1;
          if (logger.isInfoEnabled()) {
            logger.info("Executed analysis of DB results for " + q + " for " + count + " wactions in " + diff / 1000000.0D + "ms");
          }
        }
        if ((dbWactions instanceof JPAIterable)) {
          ((JPAIterable)dbWactions).close();
        }
      }
      long etime = System.nanoTime();
      long diff = etime - stime;
      Map<WactionKey, Map<String, Object>> res = q.getResults();
      if (logger.isInfoEnabled()) {
        logger.info("Executed query " + q + " with " + res.size() + " results in " + diff / 1000000.0D + "ms");
      }
      return res;
    }
    catch (Throwable t)
    {
      logger.error("Problem executing waction query using key=" + key + " fields=" + (fields == null ? "NULL" : cern.colt.Arrays.toString(fields)) + " filter=" + filter, t);
      throw t;
    }
  }
  
  Long prevCreated = null;
  Long prevCreatedRate = null;
  
  public void addSpecificMonitorEvents(MonitorEventsCollection monEvs)
  {
    Long c = Long.valueOf(this.created);
    Long rate = null;
    
    long timeStamp = monEvs.getTimeStamp();
    if (!c.equals(this.prevCreated))
    {
      monEvs.add(MonitorEvent.Type.WACTIONS_CREATED, c);
      monEvs.add(MonitorEvent.Type.LATEST_ACTIVITY, Long.valueOf(System.currentTimeMillis()));
    }
    long delta = this.prevCreated == null ? 0L : c.longValue() - this.prevCreated.longValue();
    rate = Long.valueOf(delta == 0L ? 0L : Math.ceil(1000.0D * delta / (timeStamp - this.prevTimeStamp.longValue())));
    if (!rate.equals(this.prevCreatedRate))
    {
      monEvs.add(MonitorEvent.Type.INPUT_RATE, rate);
      monEvs.add(MonitorEvent.Type.RATE, rate);
      monEvs.add(MonitorEvent.Type.WACTIONS_CREATED_RATE, rate);
    }
    this.prevCreated = c;
    this.prevCreatedRate = rate;
  }
  
  public void close()
    throws Exception
  {
    logger.info("WactionStore " + this.fullName + " is being closed");
    resetProcessThread();
    this.output.close();
    if ((expungeableWactions != null) && (memoryMonitor != null))
    {
      for (LRUNode w : expungeableWactions.toList()) {
        if (w.wactionStore == this) {
          expungeableWactions.remove(w);
        }
      }
      if (expungeableWactions.size() == 0)
      {
        memoryMonitor.stopRunning();
        memoryMonitor = null;
      }
    }
    if (this.persistedWActionType != null) {
      this.persistedWActionType.fsync();
    }
    if (this.persistencePolicy != null) {
      this.persistencePolicy.close();
    }
    if (this.persistenceLayer != null) {
      this.persistenceLayer.close();
    }
    this.persistencePolicy = null;
    
    closeCache();
    if ((this.isRecoveryEnabled) && 
      (this.persistedWActionStore != null))
    {
      WActionStoreManager manager = this.persistedWActionStore.getManager();
      CheckpointManager checkpointManager = manager.getCheckpointManager();
      checkpointManager.closeWactionStore(this.fullName);
    }
    remove(getMetaID());
    
    logger.info("WActionStore '" + this.fullName + "' is closed.");
  }
  
  public void flush()
  {
    if ((this.persistencePolicy != null) && (!this.isCrashed)) {
      this.persistencePolicy.flush();
    }
    if (this.persistedWActionType != null) {
      this.persistedWActionType.flush();
    }
  }
  
  public void stopCache()
  {
    try
    {
      if (usesInMemoryWActionStore())
      {
        this.manager.stopCache(getCacheName());
        if (this.wactions != null) {
          this.manager.stopCache(this.fullName);
        }
      }
    }
    catch (Exception e)
    {
      logger.error(e.getMessage(), e);
    }
  }
  
  public void closeCache()
  {
    try
    {
      if (usesInMemoryWActionStore())
      {
        this.manager.close(getCacheName());
        if (this.wactions != null) {
          this.manager.close(this.fullName);
        }
      }
    }
    catch (Exception e)
    {
      logger.error(e);
    }
  }
  
  public void startCache(List<UUID> servers)
  {
    try
    {
      if (usesInMemoryWActionStore())
      {
        this.manager.startCache(getCacheName(), servers);
        if (this.wactions != null) {
          this.manager.startCache(this.fullName, servers);
        }
      }
    }
    catch (Exception e)
    {
      logger.error(e);
    }
  }
  
  public String getCacheName()
  {
    return getCacheName(this.fullName);
  }
  
  public static String getCacheName(String wactionStoreName)
  {
    return wactionStoreName + "-Contexts";
  }
  
  public Waction createWaction(Object key)
  {
    Waction waction = null;
    try
    {
      Object wobj = this.wactionClass.newInstance();
      this.wactionClass.cast(wobj);
      waction = (Waction)wobj;
    }
    catch (Exception e1)
    {
      logger.error("failed to create Waction runtime class : " + this.wactionClassName + e1);
    }
    waction.init(key);
    waction.setInternalWactionStoreName(this.fullTableName);
    try
    {
      Object obj = this.wactionContextClass.newInstance();
      this.wactionContextClass.cast(obj);
      waction.setContext(obj);
    }
    catch (Exception e)
    {
      logger.error("Failed creating WactionContext runtime class : " + e);
    }
    this.created += 1L;
    return waction;
  }
  
  public void receive(Object linkID, ITaskEvent event)
    throws Exception
  {
    try
    {
      if (isFlowInError()) {
        return;
      }
      setProcessThread();
      List<WAEvent> jsonBatch = new ArrayList();
      for (WAEvent data : event.batch())
      {
        Position dataPosition = null;
        if (data.position != null)
        {
          dataPosition = data.position.createAugmentedPosition(getMetaID(), null);
          if (isBeforeWaitPosition(dataPosition))
          {
            if (!Logger.getLogger("Recovery").isDebugEnabled()) {
              continue;
            }
            Logger.getLogger("Recovery").debug("Dropping duplicate Waction: " + data.position); continue;
          }
        }
        if (Logger.getLogger("Recovery").isDebugEnabled()) {
          Logger.getLogger("Recovery").debug("Accepting new Waction: " + data.position);
        }
        SimpleEvent se = (SimpleEvent)data.data;
        Object[] keys = new Object[this.ctxKeyFacs.size()];
        for (int i = 0; i < this.ctxKeyFacs.size(); i++) {
          keys[i] = ((FieldFactory)((Pair)this.ctxKeyFacs.get(i)).second).getField(se);
        }
        Object key;
        if (keys.length == 1) {
          key = keys[0];
        } else {
          key = RecordKey.createKeyFromObjArray(keys);
        }
        if (key == null)
        {
          if (logger.isInfoEnabled()) {
            logger.info("Creating no waction because SimpleEvent has null key: " + se + " with key : " + (se == null ? "NULL!" : se.getKey()));
          }
        }
        else
        {
          Waction newWaction = createWaction(key);
          Map<String, Object> wContextMap = new HashMap();
          if ((usesInMemoryWActionStore()) && 
            (!se.allFieldsSet))
          {
            WactionContext latestContext = (WactionContext)this.latestContexts.get(key);
            if (latestContext != null) {
              wContextMap.putAll(latestContext);
            }
          }
          int i = 0;
          for (Pair<String, FieldFactory> p : this.ctxFieldFacs) {
            if (se.isFieldSet(i++))
            {
              Object val = ((FieldFactory)p.second).getField(data.data);
              wContextMap.put(p.first, val);
            }
          }
          newWaction.setContext(wContextMap);
          newWaction.context.wactionID = newWaction.uuid;
          if (usesInMemoryWActionStore()) {
            this.latestContexts.put(key, newWaction.context);
          }
          if ((se.linkedSourceEvents != null) && (this.eventBeanClasses != null)) {
            for (Object[] lses : se.linkedSourceEvents) {
              for (Object lse : lses) {
                if ((lse instanceof SimpleEvent)) {
                  if (this.eventBeanClasses.contains(lse.getClass())) {
                    newWaction.addEvent((SimpleEvent)lse);
                  }
                }
              }
            }
          }
          newWaction.setPosition(dataPosition);
          if (usesInMemoryWActionStore())
          {
            newWaction.addWactionListener(this);
            newWaction.designateCommitted();
            this.wactions.put(newWaction.getMapKey(), newWaction);
            jsonBatch.add(new WAEvent(newWaction));
          }
          if (usesOldWActionStore()) {
            if (this.persistenceAvailable) {
              this.persistencePolicy.addWaction(newWaction);
            }
          }
          if (usesNewWActionStore())
          {
            WAction jsonWA = addToWActionStore(newWaction);
            JsonNodeEvent e = new JsonNodeEvent();
            e.setData(jsonWA);
            jsonBatch.add(new WAEvent(e));
          }
        }
      }
      if (!jsonBatch.isEmpty())
      {
        TaskEvent te = TaskEvent.createStreamEvent(jsonBatch);
        this.output.publish(te);
      }
    }
    catch (Exception ex)
    {
      logger.error("exception receiving wactions by wactionstore:" + this.storeMetaInfo.getFullName());
      notifyAppMgr(EntityType.WACTIONSTORE, getMetaName(), getMetaID(), ex, "wactionstore receive", new Object[] { event });
      throw new Exception(ex);
    }
  }
  
  private synchronized boolean isBeforeWaitPosition(Position position)
  {
    if ((this.waitPosition == null) || (position == null)) {
      return false;
    }
    for (Path wactionPath : position.values()) {
      if (this.waitPosition.containsKey(wactionPath.getPathHash()))
      {
        SourcePosition sp = this.waitPosition.get(Integer.valueOf(wactionPath.getPathHash())).getSourcePosition();
        if (wactionPath.getSourcePosition().compareTo(sp) <= 0) {
          return true;
        }
        this.waitPosition = this.waitPosition.createPositionWithoutPath(wactionPath);
      }
    }
    if (this.waitPosition.isEmpty()) {
      this.waitPosition = null;
    }
    return false;
  }
  
  static class MemoryMonitor
    extends Thread
  {
    static double evictionthreshold = 0.0D;
    
    public MemoryMonitor()
    {
      evictionthreshold = Server.evictionThresholdPercent();
      setName("WactionStore-MemoryMonitor");
    }
    
    volatile boolean running = true;
    
    public void stopRunning()
    {
      this.running = false;
      WactionStore.stopEviction = true;
      interrupt();
    }
    
    public void run()
    {
      while (this.running)
      {
        double freeMemory = WactionStore.memoryFree();
        if (freeMemory < 100.0D - evictionthreshold)
        {
          if (WactionStore.logger.isInfoEnabled()) {
            WactionStore.logger.info("Hit " + (100.0D - freeMemory) + "% memory level - evicting wactions");
          }
          int count = 0;
          while ((this.running) && (freeMemory < 105.0D - evictionthreshold))
          {
            WactionStore.access$200();
            count++;
            freeMemory = WactionStore.memoryFree();
            if (count % 1000 == 0) {
              LockSupport.parkNanos(1000000L);
            }
          }
          if (WactionStore.logger.isInfoEnabled()) {
            WactionStore.logger.info("Evicted - " + count + " wactions - now at " + (100.0D - freeMemory) + "% memory level");
          }
        }
        LockSupport.parkNanos(10000000L);
      }
    }
  }
  
  static boolean reachedMaxMemory = false;
  
  private static double memoryFree()
  {
    long totalMemory = Runtime.getRuntime().totalMemory();
    long maxMemory = Runtime.getRuntime().maxMemory();
    long freeMemory = Runtime.getRuntime().freeMemory();
    boolean hadReachedMaxMemory = reachedMaxMemory;
    
    long usedMemory = totalMemory - freeMemory;
    long actualFree = maxMemory - usedMemory;
    double actualFreePercent = actualFree * 100.0D / maxMemory * 1.0D;
    if (logger.isInfoEnabled())
    {
      if ((reachedMaxMemory) && (!hadReachedMaxMemory)) {
        logger.info("Actual Free = " + actualFree + " [" + actualFreePercent + "%]: Reached max memory with " + freeMemory * 100.0D / (maxMemory * 1.0D) + "% free");
      }
      if ((!reachedMaxMemory) && (hadReachedMaxMemory)) {
        logger.info("Actual Free = " + actualFree + " [" + actualFreePercent + "%]: Downsized from max memory to " + totalMemory + " with " + freeMemory * 100.0D / (maxMemory * 1.0D) + "% free");
      }
    }
    return actualFreePercent;
  }
  
  static ReentrantLock expungeablesLock = new ReentrantLock();
  static Condition expungeablesExist = expungeablesLock.newCondition();
  static volatile boolean stopEviction = false;
  
  private static void evict()
  {
    LRUNode node = null;
    while (node == null)
    {
      node = (LRUNode)expungeableWactions.removeLeastRecentlyUsed();
      if (node != null)
      {
        node.wactionStore.wactions.remove(node.wactionKey);
        
        WactionContext c = (WactionContext)node.wactionStore.latestContexts.get(node.wactionKey.key);
        if ((c != null) && (c.wactionID != null) && (c.wactionID.equals(node.wactionKey.id))) {
          node.wactionStore.latestContexts.remove(node.wactionKey.key);
        }
      }
      else
      {
        try
        {
          expungeablesLock.lock();
          expungeablesExist.await();
        }
        catch (InterruptedException e)
        {
          if (stopEviction)
          {
            expungeablesLock.unlock(); break;
          }
        }
        finally
        {
          expungeablesLock.unlock();
        }
      }
    }
  }
  
  public void wactionAccessed(Waction waction)
  {
    if ((usesInMemoryWActionStore()) && (this.evictWactions) && ((!this.persistenceAvailable) || (waction.isPersisted())))
    {
      if (expungeableWactions == null) {
        expungeableWactions = PersistenceFactory.createExpungeList((String)this.storeMetaInfo.properties.get("wactionDef.persist.expungePolicy"));
      }
      if (memoryMonitor == null) {
        synchronized (MemoryMonitor.class)
        {
          if (memoryMonitor == null)
          {
            memoryMonitor = new MemoryMonitor();
            memoryMonitor.start();
          }
        }
      }
      WactionKey wactionKey = waction.getMapKey();
      expungeableWactions.add(new LRUNode(wactionKey, this));
      expungeablesLock.lock();
      expungeablesExist.signal();
      expungeablesLock.unlock();
    }
  }
  
  public static FieldFactory genFieldFactory(String bundleUri, WALoader wal, String eventTypeClassName, String fieldName)
    throws Exception
  {
    ClassPool pool = wal.getBundlePool(bundleUri);
    String className = "FieldFactory" + System.nanoTime();
    CtClass cc = pool.makeClass(className);
    CtClass sup = pool.get(FieldFactory.class.getName());
    cc.setSuperclass(sup);
    String code = "public Object getField(Object obj)\n{\n\t" + eventTypeClassName + " tmp = (" + eventTypeClassName + ")obj;\n" + "\treturn " + FieldToObject.genConvert(new StringBuilder().append("tmp.").append(fieldName).toString()) + ";\n" + "}\n";
    
    CtMethod m = CtNewMethod.make(code, cc);
    cc.addMethod(m);
    cc.setModifiers(cc.getModifiers() & 0xFBFF);
    cc.setModifiers(1);
    wal.addBundleClass(bundleUri, className, cc.toBytecode(), false);
    Class<?> klass = wal.loadClass(className);
    FieldFactory waf = (FieldFactory)klass.newInstance();
    return waf;
  }
  
  public String toString()
  {
    return getMetaUri() + " - " + getMetaID() + " RUNTIME";
  }
  
  public Channel getChannel()
  {
    return this.output;
  }
  
  public void start()
    throws MetaDataRepositoryException
  {
    this.isCrashed = false;
    this.crashCausingException = null;
    if (this.isRecoveryEnabled)
    {
      this.waitPosition = getPersistedCheckpointPosition();
      if (Logger.getLogger("Recovery").isDebugEnabled())
      {
        Logger.getLogger("Recovery").debug("WActionStore '" + this.fullName + "' is starting at position:");
        if (this.waitPosition != null) {
          com.bloom.utility.Utility.prettyPrint(this.waitPosition);
        }
      }
      if (this.persistedWActionStore != null)
      {
        WActionStoreManager manager = this.persistedWActionStore.getManager();
        CheckpointManager checkpointManager = manager.getCheckpointManager();
        checkpointManager.removeInvalidWActions(this.fullName);
        checkpointManager.start(this.fullName);
      }
    }
  }
  
  public Position getPersistedCheckpointPosition()
  {
    return usesOldWActionStore() ? getPersistedCheckpointPositionOld() : getPersistedCheckpointPositionNew();
  }
  
  public Position getPersistedCheckpointPositionOld()
  {
    Position result = null;
    if (this.persistenceLayer != null) {
      try
      {
        result = this.persistenceLayer.getWSPosition(getMetaNsName(), getMetaName());
      }
      catch (Exception e)
      {
        logger.error(this.fullName + ": Problem getting checkpointPosition for " + this.fullTableName, e);
        notifyAppMgr(EntityType.WACTIONSTORE, getMetaName(), getMetaID(), e, "get checkpoint position", new Object[0]);
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("WactionStore " + getMetaName() + " returning WactionStore Checkpoint from disk: " + result);
    }
    return result;
  }
  
  public Position getPersistedCheckpointPositionNew()
  {
    Position result = null;
    if ((this.isRecoveryEnabled) && (this.persistedWActionStore != null))
    {
      WActionStoreManager manager = this.persistedWActionStore.getManager();
      CheckpointManager checkpointManager = manager.getCheckpointManager();
      result = checkpointManager.get(this.fullName);
    }
    return result;
  }
  
  private MetaInfo.Flow getCurrentApp(MetaInfo.MetaObject obj)
    throws MetaDataRepositoryException
  {
    if (obj == null) {
      return null;
    }
    if (obj.type.equals(EntityType.APPLICATION)) {
      return (MetaInfo.Flow)obj;
    }
    Set<UUID> parents = obj.getReverseIndexObjectDependencies();
    if (parents != null)
    {
      MDRepository metadataRepository = MetadataRepository.getINSTANCE();
      Iterator<UUID> iter = parents.iterator();
      while (iter.hasNext())
      {
        UUID aUUID = (UUID)iter.next();
        MetaInfo.MetaObject parent = metadataRepository.getMetaObjectByUUID(aUUID, WASecurityManager.TOKEN);
        if (parent != null)
        {
          if (parent.getType() == EntityType.APPLICATION) {
            return (MetaInfo.Flow)parent;
          }
          if ((parent.getType() != EntityType.WASTOREVIEW) && (parent.getReverseIndexObjectDependencies() != null)) {
            return getCurrentApp(parent);
          }
        }
        else if (logger.isDebugEnabled())
        {
          logger.debug("Found uuid in " + obj.getFullName() + " dependencies that has no matching object: " + aUUID);
        }
      }
    }
    return null;
  }
  
  public Position getCheckpoint()
  {
    long startTime = System.currentTimeMillis();
    Position result = usesOldWActionStore() ? getCheckpointOld() : getCheckpointNew();
    long elapsed = System.currentTimeMillis() - startTime;
    com.bloom.wactionstore.Utility.reportExecutionTime(logger, elapsed);
    return result;
  }
  
  public Position getCheckpointOld()
  {
    Position result = null;
    if (this.persistencePolicy != null)
    {
      Set<Waction> toBePersisted = this.persistencePolicy.getUnpersistedWactions();
      if (toBePersisted != null) {
        for (Waction w : toBePersisted)
        {
          if (result == null) {
            result = new Position();
          }
          result.mergeLowerPositions(w.getPosition());
        }
      }
    }
    return result;
  }
  
  public Position getCheckpointNew()
  {
    if (this.isRecoveryEnabled)
    {
      WActionStoreManager manager = this.persistedWActionStore.getManager();
      CheckpointManager checkpointManager = manager.getCheckpointManager();
      checkpointManager.flush(this.persistedWActionStore);
    }
    return null;
  }
  
  public boolean clearWactionStoreCheckpoint()
  {
    if (Logger.getLogger("Recovery").isDebugEnabled()) {
      Logger.getLogger("Recovery").debug("Clearing WactionStore checkpoint for " + this.fullName);
    }
    Boolean result = Boolean.valueOf(usesOldWActionStore() ? clearWactionStoreCheckpointOld() : clearWactionStoreCheckpointNew((MetaInfo.WActionStore)getMetaInfo()));
    return result.booleanValue();
  }
  
  public boolean clearWactionStoreCheckpointOld()
  {
    if (this.persistenceLayer != null) {
      try
      {
        return this.persistenceLayer.clearWSPosition(getMetaNsName(), getMetaName());
      }
      catch (Exception e)
      {
        Logger.getLogger("Recovery").error(this.fullName + ": Problem clearing Checkpoint for " + this.fullTableName, e);
        
        return false;
      }
    }
    return true;
  }
  
  public static boolean clearWactionStoreCheckpointNew(MetaInfo.WActionStore wactionStore)
  {
    Map<String, Object> properties = wactionStore.getProperties();
    if (usesNewWActionStore(properties))
    {
      String wActionStoreName = wactionStore.getFullName();
      WActionStoreManager manager = WActionStores.getInstance(properties);
      if (manager == null)
      {
        Logger.getLogger("Recovery").warn("WactionStore not found for resetting recovery: " + wactionStore.getName());
        return false;
      }
      CheckpointManager checkpointManager = manager.getCheckpointManager();
      if (checkpointManager == null)
      {
        Logger.getLogger("Recovery").warn("WactionStore Checkpoint Manager not found for resetting recovery: " + wactionStore.getName());
        return false;
      }
      checkpointManager.writeBlankCheckpoint(wActionStoreName);
    }
    return true;
  }
  
  public static abstract class FieldFactory
  {
    public abstract Object getField(Object paramObject);
    
    public abstract String getFieldName();
  }
}
