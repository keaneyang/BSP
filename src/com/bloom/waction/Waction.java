package com.bloom.waction;

import com.bloom.classloading.WALoader;
import com.bloom.event.EventJson;
import com.bloom.uuid.UUID;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.bloom.anno.SpecialEventAttribute;
import com.bloom.event.Event;
import com.bloom.event.ObjectMapperFactory;
import com.bloom.event.SimpleEvent;
import com.bloom.recovery.Position;
import com.bloom.runtime.RecordKey;

import flexjson.JSON;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.eclipse.persistence.queries.FetchGroup;
import org.eclipse.persistence.queries.FetchGroupTracker;
import org.eclipse.persistence.sessions.Session;
import org.joda.time.DateTime;

public class Waction
  implements Serializable, FetchGroupTracker
{
  private static final long serialVersionUID = 383497383255254943L;
  private static Logger logger = Logger.getLogger(Waction.class);
  private static transient ObjectMapper mapper = ObjectMapperFactory.newInstance();
  public static final int STATUS_DEFAULT = 0;
  public static final int STATUS_COMMITTED = 2;
  public static final int STATUS_PERSISTED = 4;
  public String _id;
  public String id;
  public String internalWactionStoreName;
  public UUID uuid;
  public long wactionTs;
  public Object key;
  public WactionKey mapKey;
  public WactionContext context;
  private List<SimpleEvent> eventsNotAccessedDirectly = null;
  @JSON(include=false)
  @JsonIgnore
  private List<EventJson> jsonEventListNotAccessedDirectly = new ArrayList();
  public int wactionStatus = 0;
  @SpecialEventAttribute
  private Position position;
  transient List<WactionListener> wactionListeners = new ArrayList();
  
  public Waction()
  {
    this.context = new WactionContext();
  }
  
  public Waction(Object key)
  {
    this();
    init(key);
  }
  
  public Waction(UUID uuid, Object key, long ts, List<SimpleEvent> events, int status, WactionContext ctx)
  {
    setWactionTs(ts);
    setUuid(uuid);
    this.id = getUUIDString();
    setKey(key);
    setMapKey(new WactionKey(uuid, key));
    
    setWactionStatus(status);
    if (events != null) {
      setEvents(events);
    }
    if (ctx != null) {
      setContext(ctx);
    }
  }
  
  public void init(Object key)
  {
    setWactionTs(System.currentTimeMillis());
    setUuid(new UUID(this.wactionTs));
    this.id = getUUIDString();
    if (((key instanceof RecordKey)) && (((RecordKey)key).isEmpty())) {
      setKey(this.id);
    } else {
      setKey(key);
    }
    setMapKey(new WactionKey(this.uuid, key));
    setEvents(new ArrayList());
  }
  
  public String getId()
  {
    return getUUIDString();
  }
  
  public void setId(String id)
  {
    this.id = id;
    this.uuid = new UUID(id);
  }
  
  public String getInternalWactionStoreName()
  {
    return this.internalWactionStoreName;
  }
  
  public void setInternalWactionStoreName(String storeName)
  {
    this.internalWactionStoreName = storeName;
  }
  
  class WactionEventsClass
  {
    String eventClassName;
    String eventJson;
    
    WactionEventsClass(String eventClassName, String eventJson)
    {
      this.eventClassName = eventClassName;
      this.eventJson = eventJson;
    }
  }
  
  class WactionEventsClassList
  {
    public List<Waction.WactionEventsClass> list = new ArrayList();
    
    WactionEventsClassList() {}
    
    public void add(Waction.WactionEventsClass wec)
    {
      this.list.add(wec);
    }
  }
  
  @JSON(include=false)
  @JsonIgnore
  public byte[] getEventsString()
  {
    if ((this.eventsNotAccessedDirectly == null) || (this.eventsNotAccessedDirectly.size() < 1)) {
      return "{\"list\" :[] }".getBytes();
    }
    int ne = this.eventsNotAccessedDirectly.size();
    if (logger.isTraceEnabled()) {
      logger.trace("No of events in list to create json: " + ne);
    }
    String json = new String();
    WactionEventsClassList wecl = new WactionEventsClassList();
    try
    {
      for (int ik = 0; ik < ne; ik++)
      {
        SimpleEvent se = (SimpleEvent)this.eventsNotAccessedDirectly.get(ik);
        String eventClassName = se.getClass().getCanonicalName();
        String eventJson = se.toJSON();
        WactionEventsClass wec = new WactionEventsClass(eventClassName, eventJson);
        if (logger.isTraceEnabled()) {
          logger.trace("Event of type : " + eventClassName + ", is written as json : " + eventJson);
        }
        wecl.add(wec);
      }
      json = mapper.writeValueAsString(wecl);
    }
    catch (Exception e)
    {
      logger.error("error writing events as json" + e);
    }
    if (logger.isTraceEnabled()) {
      logger.trace("events as json string : " + json);
    }
    return json.getBytes();
  }
  
  public void setEventsString(byte[] bytes)
  {
    setEvents(new ArrayList());
    String jsonStr = new String(bytes);
    try
    {
      JsonNode rootNode = (JsonNode)mapper.readValue(jsonStr, JsonNode.class);
      if (rootNode == null)
      {
        logger.error("failed to build events list from json.");
        return;
      }
      JsonNode listNode = rootNode.get("list");
      if (listNode.isArray())
      {
        Iterator<JsonNode> nodes = listNode.elements();
        while (nodes.hasNext())
        {
          JsonNode node = (JsonNode)nodes.next();
          String eventClassName = node.get("eventClassName").asText();
          String eventNodeStr = node.get("eventJson").asText();
          Class<?> seClazz = WALoader.get().loadClass(eventClassName);
          Object seObj = seClazz.newInstance();
          seObj = seClazz.cast(seObj);
          SimpleEvent se = (SimpleEvent)((SimpleEvent)seObj).fromJSON(eventNodeStr);
          this.eventsNotAccessedDirectly.add(se);
        }
      }
    }
    catch (Exception e)
    {
      logger.error("error building events list : " + e);
    }
    if (logger.isTraceEnabled()) {
      logger.trace("no of events in events list built are : " + this.eventsNotAccessedDirectly.size());
    }
  }
  
  @JSON(include=false)
  @JsonIgnore
  public String getContextString()
  {
    String contextString = (null == this.context) || (null == this.context.toString()) ? "empty context" : this.context.toString();
    return contextString;
  }
  
  @JSON(include=false)
  @JsonIgnore
  public String getKeyString()
  {
    String keyString = (this.key == null) || (null == this.key.toString()) ? "Object key is NULL" : this.key.toString();
    return keyString;
  }
  
  public void setKeyString(String key)
  {
    this.key = key;
  }
  
  @JSON(include=false)
  @JsonIgnore
  public String getMapKeyString()
  {
    String mapKeyString = null;
    if (this.mapKey != null) {
      mapKeyString = this.mapKey.getWactionKeyStr();
    }
    return mapKeyString;
  }
  
  public void setMapKeyString(String mapKeyString)
  {
    this.mapKey = new WactionKey();
    this.mapKey.setWactionKey(mapKeyString);
  }
  
  public UUID getUuid()
  {
    return this.uuid;
  }
  
  @JSON(include=false)
  @JsonIgnore
  public String getUUIDString()
  {
    return this.uuid.getUUIDString();
  }
  
  public void setUUIDString(String uuid)
  {
    this.uuid = new UUID(uuid);
  }
  
  public void setUuid(UUID uuid)
  {
    this.uuid = uuid;
  }
  
  public long getWactionTs()
  {
    return this.wactionTs;
  }
  
  public void setWactionTs(long wactionTs)
  {
    this.wactionTs = wactionTs;
  }
  
  public Object getKey()
  {
    return this.key;
  }
  
  public void setKey(Object key)
  {
    this.key = key;
    setMapKey(new WactionKey(this.uuid, key));
  }
  
  public WactionKey getMapKey()
  {
    return this.mapKey;
  }
  
  public void setMapKey(WactionKey mapKey)
  {
    this.mapKey = mapKey;
  }
  
  @JSON(include=false)
  @JsonIgnore
  public List<EventJson> getJsonEvents()
  {
    if ((this.eventsNotAccessedDirectly == null) || (this.eventsNotAccessedDirectly.size() == 0) || (this.mapKey == null)) {
      return null;
    }
    this.jsonEventListNotAccessedDirectly = new ArrayList();
    for (SimpleEvent event : this.eventsNotAccessedDirectly)
    {
      EventJson ej = new EventJson(event);
      ej.setMapKey(this.mapKey.getWactionKeyStr());
      this.jsonEventListNotAccessedDirectly.add(ej);
    }
    if (logger.isTraceEnabled()) {
      logger.trace("no of jsonEvents in waction=" + this.mapKey.getWactionKeyStr() + ", are=" + this.jsonEventListNotAccessedDirectly.size());
    }
    return this.jsonEventListNotAccessedDirectly;
  }
  
  public void setJsonEvents(List<EventJson> jsonEvents)
  {
    if ((jsonEvents == null) || (jsonEvents.size() == 0)) {
      return;
    }
    this.jsonEventListNotAccessedDirectly = jsonEvents;
    this.eventsNotAccessedDirectly = new ArrayList();
    for (EventJson je : jsonEvents)
    {
      SimpleEvent se = (SimpleEvent)je.buildEvent();
      this.eventsNotAccessedDirectly.add(se);
    }
  }
  
  public List<SimpleEvent> getEvents()
  {
    if (((this.eventsNotAccessedDirectly == null) || (this.eventsNotAccessedDirectly.size() == 0)) && 
      (this.jsonEventListNotAccessedDirectly != null) && (this.jsonEventListNotAccessedDirectly.size() > 0)) {
      setJsonEvents(this.jsonEventListNotAccessedDirectly);
    }
    return this.eventsNotAccessedDirectly;
  }
  
  public void setEvents(List<SimpleEvent> events)
  {
    notifyWactionAccessed();
    
    this.eventsNotAccessedDirectly = events;
  }
  
  public void addEvent(SimpleEvent event)
  {
    this.eventsNotAccessedDirectly.add(event);
  }
  
  public List<Event> getEvents(int start, int stop)
  {
    List<Event> eventList = new ArrayList();
    for (int i = start; i <= stop; i++) {
      eventList.add(this.eventsNotAccessedDirectly.get(i));
    }
    return eventList;
  }
  
  @JSON(include=false)
  @JsonIgnore
  public Map<String, Object> getContext()
  {
    return this.context;
  }
  
  @JSON(include=false)
  @JsonIgnore
  public Object getContextField(String name)
  {
    return this.context.get(name);
  }
  
  public void setContext(Object obj)
  {
    if ((obj instanceof Map)) {
      setContext((Map)obj);
    }
  }
  
  public void setContext(Map<String, Object> context)
  {
    if (context != null) {
      this.context.putAll(context);
    }
  }
  
  public void setContext(WactionContext context)
  {
    this.context = context;
  }
  
  public int numberOfEvents()
  {
    if (this.eventsNotAccessedDirectly == null) {
      return 0;
    }
    return this.eventsNotAccessedDirectly.size();
  }
  
  public Object fromJSON(String json)
  {
    try
    {
      return mapper.readValue(json, getClass());
    }
    catch (Exception e) {}
    return "<Undeserializable>";
  }
  
  public String toJSON()
  {
    try
    {
      return mapper.writeValueAsString(this);
    }
    catch (Exception e) {}
    return "<Undeserializable>";
  }
  
  public JsonNode toJSONNode()
  {
    try
    {
      return (JsonNode)mapper.convertValue(this, JsonNode.class);
    }
    catch (Exception e) {}
    return null;
  }
  
  public String toString()
  {
    return " uuid = " + this.uuid.toString() + ", key =" + this.key + ", timestamp = " + this.wactionTs + ", _id = " + this._id + ", context = " + getContextString() + ", position = " + this.position;
  }
  
  public void designateCommitted()
  {
    this.wactionStatus |= 0x2;
    notifyWactionAccessed();
  }
  
  public void designatePersisted()
  {
    this.wactionStatus |= 0x4;
    notifyWactionAccessed();
  }
  
  @JSON(include=false)
  @JsonIgnore
  public boolean isPersisted()
  {
    return (this.wactionStatus & 0x4) == 4;
  }
  
  public int getWactionStatus()
  {
    return this.wactionStatus;
  }
  
  public void setWactionStatus(int wactionStatus)
  {
    this.wactionStatus = wactionStatus;
  }
  
  public void addWactionListener(WactionListener listener)
  {
    this.wactionListeners.add(listener);
  }
  
  protected Object getWrappedValue(boolean val)
  {
    return new Boolean(val);
  }
  
  protected Object getWrappedValue(char val)
  {
    return new Character(val);
  }
  
  protected Object getWrappedValue(short val)
  {
    return new Short(val);
  }
  
  protected Object getWrappedValue(int val)
  {
    return new Integer(val);
  }
  
  protected Object getWrappedValue(long val)
  {
    return new Long(val);
  }
  
  protected Object getWrappedValue(float val)
  {
    return new Float(val);
  }
  
  protected Object getWrappedValue(double val)
  {
    return new Double(val);
  }
  
  protected Object getWrappedValue(Object val)
  {
    return val;
  }
  
  protected String getWrappedValue(String val)
  {
    if (val == null) {
      return null;
    }
    return new String(val);
  }
  
  protected Object getWrappedValue(DateTime val)
  {
    return val;
  }
  
  private boolean notificationFlag = false;
  
  public synchronized void notifyWactionAccessed()
  {
    if (this.notificationFlag == true) {
      return;
    }
    this.notificationFlag = true;
    for (WactionListener listener : this.wactionListeners) {
      listener.wactionAccessed(this);
    }
    this.notificationFlag = false;
  }
  
  public Position getPosition()
  {
    return this.position;
  }
  
  public void setPosition(Position position)
  {
    this.position = position;
  }
  
  transient FetchGroup fetchGroup = null;
  transient Session session;
  
  public FetchGroup _persistence_getFetchGroup()
  {
    return this.fetchGroup;
  }
  
  public void _persistence_setFetchGroup(FetchGroup group)
  {
    this.fetchGroup = group;
  }
  
  public boolean _persistence_isAttributeFetched(String attribute)
  {
    boolean fetched = false;
    if (this.fetchGroup != null) {
      fetched = this.fetchGroup.containsAttribute(attribute);
    }
    return fetched;
  }
  
  public void _persistence_resetFetchGroup() {}
  
  public boolean _persistence_shouldRefreshFetchGroup()
  {
    return false;
  }
  
  public void _persistence_setShouldRefreshFetchGroup(boolean shouldRefreshFetchGroup) {}
  
  public Session _persistence_getSession()
  {
    return this.session;
  }
  
  public void _persistence_setSession(Session session)
  {
    this.session = session;
  }
}
