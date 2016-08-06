package com.bloom.metaRepository;

import com.bloom.runtime.Pair;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfoStatus;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.Query;
import com.bloom.runtime.meta.MetaInfo.Server;
import com.bloom.runtime.meta.MetaInfo.ShowStream;
import com.bloom.runtime.meta.MetaInfo.StatusInfo;
import com.bloom.runtime.meta.MetaInfo.Type;
import com.bloom.runtime.utils.NamePolicy;
import com.bloom.uuid.UUID;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.core.MessageListener;
import com.hazelcast.core.MultiMap;
import com.bloom.event.ObjectMapperFactory;
import com.bloom.recovery.Position;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EventListener;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class MDCache
  implements MDCacheInterface, MembershipListener
{
  public static final int FIRST_VERSION = 1;
  private static MDCache INSTANCE = new MDCache(false);
  private static Logger logger = Logger.getLogger(MDCache.class);
  private boolean areMapsLoaded;
  private IMap<UUID, String> uuidToURL;
  private IMap<String, MetaInfo.MetaObject> urlToMetaObject;
  private MultiMap<Integer, String> eTypeToMetaObject;
  private IMap<UUID, MetaInfo.Server> servers;
  private IMap<UUID, MetaInfo.StatusInfo> status;
  private ITopic<MetaInfo.ShowStream> showStream;
  private MultiMap<UUID, UUID> deploymentInfo;
  private IMap<UUID, Position> positionInfo;
  private CacheStatistics statistics;
  private HazelcastInstance hz;
  
  private MDCache()
  {
    if (INSTANCE != null) {
      throw new IllegalStateException("An instance of MDCache already exists");
    }
  }
  
  private MDCache(boolean areMapsLoaded)
  {
    this.areMapsLoaded = areMapsLoaded;
    this.statistics = new CacheStatistics();
  }
  
  public static MDCache getInstance()
  {
    if (!INSTANCE.areMapsLoaded) {
      INSTANCE.initMaps();
    }
    return INSTANCE;
  }
  
  public static MDCache getInstance(String key, String value)
  {
    if (!INSTANCE.areMapsLoaded)
    {
      INSTANCE.setInstanceProperties(key, value);
      INSTANCE.initMaps();
    }
    return INSTANCE;
  }
  
  public void setInstanceProperties(String key, String value)
  {
    System.setProperty(key, value);
  }
  
  private void initMaps()
  {
    if (!this.areMapsLoaded)
    {
      if (logger.isDebugEnabled()) {
        logger.debug("Loading IMaps first time from DB");
      }
      this.hz = HazelcastSingleton.get();
      this.uuidToURL = this.hz.getMap("#uuidToUrl");
      this.urlToMetaObject = this.hz.getMap("#urlToMetaObject");
      this.eTypeToMetaObject = this.hz.getMultiMap("#eTypeToMetaObject");
      this.status = this.hz.getMap("#status");
      this.showStream = this.hz.getTopic("#showStream");
      this.deploymentInfo = this.hz.getMultiMap("deployedObjects");
      this.servers = this.hz.getMap("#servers");
      this.positionInfo = this.hz.getMap("#PositionInfo");
      this.areMapsLoaded = true;
      
      this.hz.getCluster().addMembershipListener(this);
    }
  }
  
  public CacheStatistics getStatistics()
  {
    return this.statistics;
  }
  
  public boolean put(Object mObject)
  {
    long start = System.currentTimeMillis();
    boolean success;
    if ((mObject instanceof MetaInfo.MetaObject))
    {
      if ((mObject instanceof MetaInfo.Server))
      {
        MetaInfo.MetaObject metaObject = (MetaInfo.MetaObject)mObject;
        this.servers.put(metaObject.getUuid(), (MetaInfo.Server)mObject);
        this.eTypeToMetaObject.put(Integer.valueOf(((MetaInfo.Server)mObject).type.ordinal()), ((MetaInfo.MetaObject)mObject).getUri());
        
        success = this.servers.containsKey(metaObject.getUuid()) == true;
      }
      else
      {
        success = putMetaObjectInAllMaps((MetaInfo.MetaObject)mObject);
      }
    }
    else
    {
      if ((mObject instanceof MetaInfo.StatusInfo))
      {
        success = putStatusInfo((MetaInfo.StatusInfo)mObject);
      }
      else
      {
        if ((mObject instanceof MetaInfo.ShowStream))
        {
          this.showStream.publish((MetaInfo.ShowStream)mObject);
          success = true;
        }
        else
        {
          if ((mObject instanceof Pair))
          {
            Pair<UUID, UUID> pair = (Pair)mObject;
            success = this.deploymentInfo.put(pair.first, pair.second);
          }
          else
          {
            if ((mObject instanceof Map.Entry))
            {
              Map.Entry<?, ?> entry = (Map.Entry)mObject;
              UUID key = null;
              Position position = null;
              if (((entry.getKey() instanceof UUID)) && ((entry.getValue() instanceof Position)))
              {
                key = (UUID)entry.getKey();
                position = (Position)entry.getValue();
                this.positionInfo.put(key, position);
                success = this.positionInfo.get(key) == position;
              }
              else
              {
                success = false;
              }
            }
            else
            {
              if (mObject == null) {
                throw new NullPointerException("Expects a non NULL Object to store in MetaData Cache!");
              }
              throw new UnsupportedOperationException("Unexpcted Object type : " + mObject.getClass().toString() + ", will not store in MetaData Cache!");
            }
          }
        }
      }
    }
    this.statistics.cachePuts += 1L;
    this.statistics.totalPutMillis += System.currentTimeMillis() - start;
    return success;
  }
  
  private boolean putMetaObjectInAllMaps(MetaInfo.MetaObject mObject)
  {
    boolean success = true;
    String uriInCaps = NamePolicy.makeKey(mObject.getUri());
    this.uuidToURL.put(mObject.getUuid(), uriInCaps);
    this.urlToMetaObject.put(uriInCaps, mObject);
    if (!mObject.getMetaInfoStatus().isDropped()) {
      this.eTypeToMetaObject.put(Integer.valueOf(mObject.type.ordinal()), mObject.getUri());
    }
    if ((!this.uuidToURL.containsKey(mObject.uuid)) || (!this.urlToMetaObject.containsKey(mObject.getUri())) || (!this.eTypeToMetaObject.containsKey(Integer.valueOf(mObject.type.ordinal())))) {
      success = false;
    }
    if (logger.isDebugEnabled()) {
      logger.debug(this.uuidToURL.entrySet() + "\n" + this.urlToMetaObject.entrySet());
    }
    return success;
  }
  
  private boolean putStatusInfo(MetaInfo.StatusInfo mObject)
  {
    if (logger.isDebugEnabled()) {
      logger.debug("Putting status for " + mObject.type + " : " + mObject.OID);
    }
    boolean success = true;
    this.status.put(mObject.getOID(), mObject);
    if (!this.status.containsKey(mObject.getOID())) {
      success = false;
    }
    return success;
  }
  
  public Object get(EntityType eType, UUID uuid, String namespace, String name, Integer version, MDConstants.typeOfGet get)
  {
    long start = System.currentTimeMillis();
    Object result = null;
    switch (get)
    {
    case BY_NAME: 
      result = getByName(eType, namespace, name, version);
      break;
    case BY_ENTITY_TYPE: 
      result = getByEntityType(eType);
      break;
    case BY_NAMESPACE: 
      result = getByNamespace(namespace, null);
      break;
    case BY_NAMESPACE_AND_ENTITY_TYPE: 
      result = getByNamespace(namespace, eType);
      break;
    case BY_UUID: 
      result = getByUUID(uuid, name);
      break;
    }
    if (result == null)
    {
      this.statistics.cacheMisses += 1L;
      this.statistics.totalGetMillis += System.currentTimeMillis() - start;
    }
    else
    {
      this.statistics.cacheHits += 1L;
      this.statistics.totalGetMillis += System.currentTimeMillis() - start;
    }
    return result;
  }
  
  private MetaInfo.MetaObject getByName(EntityType eType, String namespace, String name, Integer version)
  {
    String vUrl = MDConstants.makeURLWithVersion(eType, namespace, name, version);
    if (logger.isDebugEnabled()) {
      logger.debug("Looking for: " + vUrl);
    }
    return (MetaInfo.MetaObject)this.urlToMetaObject.get(vUrl);
  }
  
  private Set getByEntityType(EntityType eType)
  {
    Set<MetaInfo.MetaObject> objSet = null;
    Collection<String> resultCollection = this.eTypeToMetaObject.get(Integer.valueOf(eType.ordinal()));
    if (resultCollection != null)
    {
      if (eType == EntityType.SERVER) {
        return new HashSet(this.servers.values());
      }
      Set<MetaInfo.MetaObject> mSet = new HashSet();
      for (String muri : resultCollection)
      {
        MetaInfo.MetaObject mObject = (MetaInfo.MetaObject)getUrlToMetaObject().get(muri);
        mSet.add(mObject);
      }
      return mSet;
    }
    return null;
  }
  
  private Set<MetaInfo.MetaObject> getByNamespace(String namespace, EntityType eType)
  {
    Set<MetaInfo.MetaObject> lObjects = null;
    String searchTag;
    if (eType != null)
    {
      searchTag = NamePolicy.makeKey(namespace + ":" + eType);
      if (logger.isDebugEnabled()) {
        logger.debug("search Tag : " + searchTag);
      }
      for (String tt : this.urlToMetaObject.keySet()) {
        if (tt.startsWith(searchTag))
        {
          if (lObjects == null) {
            lObjects = new HashSet();
          }
          lObjects.add(this.urlToMetaObject.get(tt));
        }
      }
    }
    else
    {
      searchTag = NamePolicy.makeKey(namespace + ":");
      if (logger.isDebugEnabled()) {
        logger.debug("search Tag : " + searchTag);
      }
      for (String tt : this.urlToMetaObject.keySet()) {
        if (tt.startsWith(searchTag))
        {
          if (lObjects == null) {
            lObjects = new HashSet();
          }
          lObjects.add(this.urlToMetaObject.get(tt));
        }
      }
    }
    return lObjects;
  }
  
  private Object getByUUID(UUID uuid, String status)
  {
    Object got = null;
    if ((status != null) && (status.equalsIgnoreCase("status")))
    {
      got = this.status.get(uuid);
    }
    else if ((status != null) && (status.equalsIgnoreCase("serversForDeployment")))
    {
      got = this.deploymentInfo.get(uuid);
    }
    else
    {
      String vUrl = (String)this.uuidToURL.get(uuid);
      if (vUrl != null) {
        got = this.urlToMetaObject.get(vUrl);
      }
      if (got == null) {
        got = this.servers.get(uuid);
      }
    }
    return got;
  }
  
  public Object remove(EntityType eType, Object uuid, String namespace, String name, Integer version, MDConstants.typeOfRemove remove)
  {
    long start = System.currentTimeMillis();
    Object removed = null;
    switch (remove)
    {
    case BY_NAME: 
      removed = removeByName(eType, namespace, name, version);
      break;
    case BY_UUID: 
      removed = removeByUUID(uuid, name);
      break;
    }
    if (removed != null) {
      this.statistics.cacheRemovals += 1L;
    }
    this.statistics.totalRemovalsMillis += System.currentTimeMillis() - start;
    return removed;
  }
  
  private Object removeByName(EntityType eType, String namespace, String name, Integer version)
  {
    Object mObject = get(eType, null, namespace, name, version, MDConstants.typeOfGet.BY_NAME);
    if (mObject != null) {
      return removeMetaObjectFromAllMaps(mObject);
    }
    return mObject;
  }
  
  private Object removeByUUID(Object objectID, String name)
  {
    Object removed = null;
    if ((objectID instanceof UUID))
    {
      Object mObject;
      if ((name != null) && (name.equalsIgnoreCase("status"))) {
        mObject = get(null, (UUID)objectID, null, name, null, MDConstants.typeOfGet.BY_UUID);
      } else {
        mObject = get(null, (UUID)objectID, null, null, null, MDConstants.typeOfGet.BY_UUID);
      }
      if (mObject != null) {
        removed = removeMetaObjectFromAllMaps(mObject);
      }
    }
    else if ((objectID instanceof Pair))
    {
      Pair<UUID, UUID> pair = (Pair)objectID;
      Object toBeRemoved = this.deploymentInfo.get(pair.first);
      if (toBeRemoved != null)
      {
        Iterator iterator = ((Collection)toBeRemoved).iterator();
        if (iterator.hasNext())
        {
          removed = removeMetaObjectFromAllMaps(pair);
          if (logger.isDebugEnabled())
          {
            logger.debug("Removed : " + removed);
            if (((Boolean)removed).booleanValue()) {
              logger.debug("Removed Pair : " + pair);
            } else {
              logger.debug("Could not remove Pair : " + pair);
            }
          }
        }
      }
    }
    else
    {
      logger.error("Unknown type, can't remove based on Object ID.");
    }
    return removed;
  }
  
  private Object removeMetaObjectFromAllMaps(Object object)
  {
    if ((object instanceof MetaInfo.MetaObject))
    {
      MetaInfo.MetaObject mObject = (MetaInfo.MetaObject)object;
      if ((mObject instanceof MetaInfo.Server))
      {
        this.servers.remove(mObject.getUuid());
      }
      else
      {
        this.uuidToURL.remove(mObject.getUuid());
        this.urlToMetaObject.remove(mObject.getUri());
      }
      this.eTypeToMetaObject.remove(Integer.valueOf(mObject.getType().ordinal()), mObject.getUri());
      return mObject;
    }
    if ((object instanceof MetaInfo.StatusInfo))
    {
      MetaInfo.StatusInfo statusInfo = (MetaInfo.StatusInfo)object;
      return this.status.remove(statusInfo.getOID());
    }
    if ((object instanceof Pair))
    {
      Pair<UUID, UUID> pair = (Pair)object;
      return Boolean.valueOf(this.deploymentInfo.remove(pair.first, pair.second));
    }
    logger.error("Trying to remove object of unknown type: " + object.getClass().toString());
    return null;
  }
  
  public boolean update(MetaInfo.MetaObject metaObject)
  {
    if (this.urlToMetaObject.containsKey(metaObject.getUri()))
    {
      this.urlToMetaObject.put(metaObject.getUri(), metaObject);
      if (metaObject.getMetaInfoStatus().isDropped()) {
        this.eTypeToMetaObject.remove(Integer.valueOf(metaObject.getType().ordinal()), metaObject.getUri());
      }
      return true;
    }
    return false;
  }
  
  public boolean contains(MDConstants.typeOfRemove contains, UUID uuid, String url)
  {
    switch (contains)
    {
    case BY_UUID: 
      return this.uuidToURL.containsKey(uuid);
    case BY_NAME: 
      return this.urlToMetaObject.containsKey(url);
    }
    return false;
  }
  
  public boolean clear()
  {
    boolean cleaned = true;
    this.uuidToURL.clear();
    this.urlToMetaObject.clear();
    this.eTypeToMetaObject.clear();
    if ((!this.uuidToURL.isEmpty()) || (!this.urlToMetaObject.isEmpty()) || (this.eTypeToMetaObject.size() != 0)) {
      cleaned = false;
    }
    return cleaned;
  }
  
  public String registerListerForDeploymentInfo(EntryListener<UUID, UUID> entryListener)
  {
    return this.deploymentInfo.addEntryListener(entryListener, true);
  }
  
  public String registerListenerForShowStream(MessageListener<MetaInfo.ShowStream> messageListener)
  {
    return this.showStream.addMessageListener(messageListener);
  }
  
  public String registerListenerForStatusInfo(EntryListener<UUID, MetaInfo.StatusInfo> entryListener)
  {
    return this.status.addEntryListener(entryListener, true);
  }
  
  public String registerListenerForMetaObject(EntryListener entryListener)
  {
    return this.urlToMetaObject.addEntryListener(entryListener, true);
  }
  
  public String registerListenerForServer(EntryListener<UUID, MetaInfo.Server> entryListener)
  {
    return this.servers.addEntryListener(entryListener, true);
  }
  
  public void removeListerForDeploymentInfo(String registrationId)
  {
    this.deploymentInfo.removeEntryListener(registrationId);
  }
  
  public void removeListenerForShowStream(String registrationid)
  {
    this.showStream.removeMessageListener(registrationid);
  }
  
  public void removeListenerForStatusInfo(String registrationId)
  {
    this.status.removeEntryListener(registrationId);
  }
  
  public void removeListenerForMetaObject(String registrationId)
  {
    this.urlToMetaObject.removeEntryListener(registrationId);
  }
  
  public void removeListenerForServer(String registrationId)
  {
    this.servers.removeEntryListener(registrationId);
  }
  
  public boolean shutDown()
  {
    INSTANCE = null;
    return INSTANCE == null;
  }
  
  public void reset()
  {
    INSTANCE.areMapsLoaded = false;
    INSTANCE.initMaps();
  }
  
  public String registerCacheEntryListener(EventListener eventListener, MDConstants.mdcListener listener)
  {
    switch (listener)
    {
    case deploymentInfo: 
      return this.deploymentInfo.addEntryListener((EntryListener)eventListener, true);
    case showStream: 
      return this.showStream.addMessageListener((MessageListener)eventListener);
    case status: 
      return this.status.addEntryListener((EntryListener)eventListener, true);
    case urlToMeta: 
      return this.urlToMetaObject.addEntryListener((EntryListener)eventListener, true);
    case server: 
      return this.servers.addEntryListener((EntryListener)eventListener, true);
    }
    return null;
  }
  
  public String exportMetadataAsJson()
  {
    try
    {
      ObjectMapper jsonMapper = ObjectMapperFactory.newInstance();
      jsonMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
      if (logger.isInfoEnabled()) {
        logger.info("urlToMetaObject.size : " + this.urlToMetaObject.size());
      }
      String json = getjsonfrommetaobject();
      logger.warn("Number of Meta objects exported as JSON: " + this.urlToMetaObject.size() + ", and json size:" + json.length());
      if (logger.isInfoEnabled()) {
        logger.info("json doc size : " + json.length());
      }
      return json;
    }
    catch (JsonGenerationException e)
    {
      logger.error(e);
    }
    catch (JsonMappingException e)
    {
      logger.error(e);
    }
    catch (JSONException e)
    {
      logger.error(e);
    }
    catch (IOException e)
    {
      logger.error(e);
    }
    return null;
  }
  
  public String getjsonfrommetaobject()
    throws JsonProcessingException, JSONException
  {
    Map<String, JSONObject> map = new Hashtable();
    String json = "";
    Set<String> keys = this.urlToMetaObject.keySet();
    if ((keys == null) || (keys.size() == 0))
    {
      logger.warn("no meta objects exists");
      return json;
    }
    ObjectMapper jsonMapper = ObjectMapperFactory.newInstance();
    jsonMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
    List<String> ignoredList = Arrays.asList(new String[] { "GLOBAL:TYPE:MONITORBATCHEVENT", "GLOBAL:SOURCE:", "GLOBAL:STREAM:", "GLOBAL:CQ:", "GLOBAL:APPLICATION:", "GLOBAL:FLOW:" });
    for (String key : keys)
    {
      boolean skipKey = false;
      for (int i = 0; i < ignoredList.size(); i++) {
        if (key.startsWith((String)ignoredList.get(i)))
        {
          skipKey = true;
          break;
        }
      }
      if (!skipKey)
      {
        json = json + "\"" + key + "\" : ";
        if (key.contains(":" + EntityType.QUERY.name().toUpperCase() + ":"))
        {
          json = json + ((MetaInfo.Query)this.urlToMetaObject.get(key)).JSONify();
        }
        else if (key.contains(":" + EntityType.TYPE.name().toUpperCase() + ":"))
        {
          String result = ((MetaInfo.Type)this.urlToMetaObject.get(key)).JSONifyString();
          json = json + result;
        }
        else
        {
          json = json + jsonMapper.writeValueAsString(this.urlToMetaObject.get(key));
        }
        json = json + ",";
      }
    }
    json = json.substring(0, json.length() - 1);
    return "{\n" + json + "\n}";
  }
  
  public void importMetadataFromJson(String json, Boolean replace)
  {
    try
    {
      ObjectMapper jsonMapper = ObjectMapperFactory.newInstance();
      JsonNode listNode = jsonMapper.readTree(json);
      if (!listNode.isContainerNode())
      {
        logger.warn("JSON MetaData is not in list format");
        return;
      }
      Set<MetaInfo.MetaObject> moSet = new HashSet();
      Iterator<JsonNode> it = listNode.elements();
      while (it.hasNext()) {
        try
        {
          JsonNode moNode = (JsonNode)it.next();
          String className = moNode.get("metaObjectClass").asText();
          
          String moNodeText = moNode.toString();
          Class<?> moClass = Class.forName(className);
          
          Object ob = jsonMapper.readValue(moNodeText, moClass);
          MetaInfo.MetaObject mo = (MetaInfo.MetaObject)ob;
          
          moSet.add(mo);
        }
        catch (ClassNotFoundException e)
        {
          logger.error(e, e);
        }
        catch (Exception e)
        {
          logger.error(e.getMessage());
        }
      }
      if (replace.booleanValue() == true) {
        this.urlToMetaObject.clear();
      }
      logger.warn("----- MDC READY TO IMPORT OBJECTS: #" + moSet.size());
      EntityType[] orderedTypes = { EntityType.USER, EntityType.ROLE, EntityType.TYPE, EntityType.STREAM, EntityType.CQ, EntityType.TYPE, EntityType.SOURCE, EntityType.TARGET, EntityType.WI, EntityType.PROPERTYSET, EntityType.WACTIONSTORE, EntityType.CACHE, EntityType.SERVER, EntityType.INITIALIZER, EntityType.DG, EntityType.VISUALIZATION, EntityType.WINDOW, EntityType.STREAM, EntityType.APPLICATION, EntityType.NAMESPACE };
      for (EntityType entityType : orderedTypes)
      {
        Set<MetaInfo.MetaObject> typedMos = new HashSet();
        for (MetaInfo.MetaObject mo : moSet) {
          if (mo.type == entityType) {
            typedMos.add(mo);
          }
        }
        for (MetaInfo.MetaObject mo : typedMos)
        {
          logger.warn("----- MDC IMPORT type=" + entityType + " mo=" + mo);
          
          this.urlToMetaObject.put(mo.uri, mo);
        }
        moSet.removeAll(typedMos);
      }
      for (MetaInfo.MetaObject mo : moSet)
      {
        logger.warn("----- MDC IMPORT THE REST mo=" + mo);
        
        this.urlToMetaObject.put(mo.uri, mo);
      }
    }
    catch (JsonProcessingException e)
    {
      logger.error(e, e);
    }
    catch (IOException e)
    {
      logger.error(e, e);
    }
  }
  
  public IMap<UUID, String> getUuidToURL()
  {
    return this.uuidToURL;
  }
  
  public IMap<String, MetaInfo.MetaObject> getUrlToMetaObject()
  {
    return this.urlToMetaObject;
  }
  
  private MultiMap<Integer, String> geteTypeToMetaObject()
  {
    return this.eTypeToMetaObject;
  }
  
  public IMap<UUID, MetaInfo.StatusInfo> getStatus()
  {
    return this.status;
  }
  
  public ITopic<MetaInfo.ShowStream> getShowStream()
  {
    return this.showStream;
  }
  
  public MultiMap<UUID, UUID> getDeploymentInfo()
  {
    return this.deploymentInfo;
  }
  
  public IMap<UUID, Position> getPositionInfo()
  {
    return this.positionInfo;
  }
  
  public void memberAdded(MembershipEvent event) {}
  
  public void memberAttributeChanged(MemberAttributeEvent event) {}
  
  public void memberRemoved(MembershipEvent event)
  {
    UUID leftUUID = new UUID(event.getMember().getUuid());
    List<Pair<UUID, UUID>> toRemove = new ArrayList();
    for (Map.Entry<UUID, UUID> entry : this.deploymentInfo.entrySet()) {
      if (leftUUID.equals(entry.getValue())) {
        toRemove.add(Pair.make(entry.getKey(), entry.getValue()));
      }
    }
    for (Pair<UUID, UUID> pair : toRemove) {
      this.deploymentInfo.remove(pair.first, pair.second);
    }
  }
}
