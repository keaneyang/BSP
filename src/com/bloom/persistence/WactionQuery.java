package com.bloom.persistence;

import com.bloom.classloading.WALoader;
import com.bloom.distribution.Queryable;
import com.bloom.distribution.WAQuery;
import com.bloom.distribution.WASimpleQuery;
import com.bloom.distribution.WAQuery.ResultStats;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Type;
import com.bloom.runtime.utils.NamePolicy;
import com.bloom.uuid.UUID;
import com.bloom.waction.Waction;
import com.bloom.waction.WactionKey;
import com.bloom.event.SimpleEvent;

import java.io.PrintStream;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

public class WactionQuery
  implements WAQuery<WactionKey, Waction>, WASimpleQuery<WactionKey, Waction>, Serializable
{
  private static final long serialVersionUID = -2540403017766014356L;
  private static Logger logger = Logger.getLogger(WAQuery.class);
  String wsName;
  String[] fields;
  Map<String, Object> filterMap;
  Map<String, FilterCondition> contextFilter;
  Long startTime = null;
  Long endTime = null;
  Object key = null;
  WactionKey singleKey = null;
  boolean getLatestPerKey = true;
  String sortBy = null;
  boolean sortDescending = true;
  int limit = -1;
  Integer maxResults = null;
  boolean useMaxResults = false;
  String groupBy = null;
  boolean getEvents = false;
  transient Map<WactionKey, Map<String, Object>> finalResults = new TreeMap();
  transient List<Waction> simpleQueryResults = new ArrayList();
  WAQuery.ResultStats globalStats = null;
  transient Map<String, Map<Long, Map<String, Object>>> sampledResults = null;
  transient TreeSet<Long> timePeriods;
  boolean doSampling = false;
  transient Map<Object, Map<String, Object>> unique = new HashMap();
  transient Map<Object, WactionKey> mapper = new HashMap();
  transient ResultMapComparator rmc = null;
  
  public static enum OpType
  {
    IN,  BTWN,  LIKE,  GT,  LT,  GTE,  LTE,  EQ,  NE;
    
    private OpType() {}
  }
  
  public static class FilterCondition
    implements Serializable
  {
    private static final long serialVersionUID = -757349114310144139L;
    WactionQuery.OpType opType;
    Set<Object> inData = new HashSet();
    Object comp1;
    Object comp2;
    
    public FilterCondition(String condition, Class<?> valueClass)
    {
      WactionQuery.OpType[] values = WactionQuery.OpType.values();
      for (int i = 0; i < values.length; i++)
      {
        WactionQuery.OpType ot = values[i];
        String id = "$" + ot + "$";
        if (condition.startsWith(id))
        {
          if (condition.length() == id.length()) {
            throw new RuntimeException("Condition type " + ot + " requires parameters in " + condition);
          }
          String params = condition.substring(id.length());
          String[] paramSplit = params.split("[~]");
          Object[] objs;
          if (!String.class.equals(valueClass))
          {
             objs = new Object[paramSplit.length];
            try
            {
              Constructor c = valueClass.getConstructor(new Class[] { String.class });
              for (int j = 0; j < paramSplit.length; j++) {
                objs[j] = c.newInstance(new Object[] { paramSplit[j] });
              }
            }
            catch (NoSuchMethodException|SecurityException|InstantiationException|IllegalAccessException|IllegalArgumentException|InvocationTargetException e)
            {
              throw new RuntimeException("Cannot generate condition " + condition + " on " + valueClass.getSimpleName() + ":" + e.getMessage());
            }
          }
          else
          {
            objs = paramSplit;
          }
          this.opType = ot;
          setData(objs);
          break;
        }
      }
      if (this.opType == null)
      {
        this.opType = WactionQuery.OpType.EQ;
        setData(new Object[] { condition });
      }
    }
    
    public void setData(Object[] data)
    {
      switch (this.opType.ordinal())
      {
      case 1: 
        if (data.length == 0) {
          throw new RuntimeException("IN operator needs 1 or more parameter");
        }
        for (Object o : data) {
          this.inData.add(o);
        }
        break;
      case 2: 
        if (data.length != 2) {
          throw new RuntimeException("BTWN operator needs 2 parameters");
        }
        this.comp1 = data[0];
        this.comp2 = data[1];
        break;
      default: 
        if (data.length != 1) {
          throw new RuntimeException(this + " operator needs 1 parameter");
        }
        this.comp1 = data[0];
      }
    }
    
    public boolean evaluate(Object val)
    {
      if (val == null) {
        return false;
      }
      switch (this.opType.ordinal())
      {
      case 1: 
        return this.inData.contains(val);
      case 2: 
        if ((val instanceof Comparable)) {
          return (((Comparable)val).compareTo(this.comp1) >= 0) && (((Comparable)val).compareTo(this.comp2) <= 0);
        }
        return false;
      case 3: 
        return val.toString().toLowerCase().contains(this.comp1.toString().toLowerCase());
      case 4: 
        if ((val instanceof Comparable)) {
          return ((Comparable)val).compareTo(this.comp1) > 0;
        }
        return false;
      case 5: 
        if ((val instanceof Comparable)) {
          return ((Comparable)val).compareTo(this.comp1) >= 0;
        }
        return false;
      case 6: 
        if ((val instanceof Comparable)) {
          return ((Comparable)val).compareTo(this.comp1) < 0;
        }
        return false;
      case 7: 
        if ((val instanceof Comparable)) {
          return ((Comparable)val).compareTo(this.comp1) <= 0;
        }
        return false;
      case 8: 
        return val.equals(this.comp1);
      case 9: 
        return !val.equals(this.comp1);
      }
      return false;
    }
    
    public String quoteIfString(Object obj)
    {
      if ((obj instanceof String)) {
        return "'" + obj + "'";
      }
      return obj.toString();
    }
    
    public String toString()
    {
      switch (this.opType.ordinal())
      {
      case 2: 
        return "BETWEEN " + quoteIfString(this.comp1) + " AND " + quoteIfString(this.comp2);
      case 8: 
        return "= " + quoteIfString(this.comp1);
      case 4: 
        return "> " + quoteIfString(this.comp1);
      case 5: 
        return ">= " + quoteIfString(this.comp1);
      case 1: 
        String in = "IN (";
        boolean first = true;
        for (Object inDatum : this.inData)
        {
          if (!first) {
            in = in + ", ";
          } else {
            first = false;
          }
          in = in + quoteIfString(inDatum);
        }
        in = in + ")";
        return in;
      case 3: 
        return "LIKE '%" + this.comp1 + "%'";
      case 6: 
        return "< " + quoteIfString(this.comp1);
      case 7: 
        return "<= " + quoteIfString(this.comp1);
      case 9: 
        return "<> " + quoteIfString(this.comp1);
      }
      return null;
    }
  }
  
  private Map<String, String> primToClassMapping = null;
  
  private String getBoxedClassName(String className)
  {
    if (this.primToClassMapping == null)
    {
      this.primToClassMapping = new HashMap();
      this.primToClassMapping.put(Byte.TYPE.getName(), Byte.class.getName());
      this.primToClassMapping.put(Short.TYPE.getName(), Short.class.getName());
      this.primToClassMapping.put(Integer.TYPE.getName(), Integer.class.getName());
      this.primToClassMapping.put(Long.TYPE.getName(), Long.class.getName());
      this.primToClassMapping.put(Float.TYPE.getName(), Float.class.getName());
      this.primToClassMapping.put(Double.TYPE.getName(), Double.class.getName());
      this.primToClassMapping.put(Boolean.TYPE.getName(), Boolean.class.getName());
    }
    if (this.primToClassMapping.containsKey(className)) {
      return (String)this.primToClassMapping.get(className);
    }
    return className;
  }
  
  public WactionQuery(String wsName, long _startTime, long _endTime)
  {
    this.wsName = wsName;
    this.startTime = Long.valueOf(_startTime);
    if (this.startTime.longValue() < 100000000000L) {
      this.startTime = Long.valueOf(this.startTime.longValue() * 1000L);
    }
    this.endTime = Long.valueOf(_endTime);
    if (this.endTime.longValue() < 100000000000L) {
      this.endTime = Long.valueOf(this.endTime.longValue() * 1000L);
    }
    this.getLatestPerKey = false;
  }
  
  public WactionQuery(String wsName, MetaInfo.Type contextType, String[] fields, Map<String, Object> filter)
  {
    this.wsName = wsName;
    if (fields != null) {
      this.fields = ((String[])fields.clone());
    } else {
      this.fields = new String[] { "default" };
    }
    this.filterMap = filter;
    if (filter != null)
    {
      Map<String, Object> insensitiveMap = makeCaseInsenstiveMap(filter);
      Object startTimeVal = insensitiveMap.get("startTime");
      if (startTimeVal != null)
      {
        this.startTime = Long.valueOf(new Double(startTimeVal.toString()).longValue());
        if (this.startTime.longValue() < 100000000000L) {
          this.startTime = Long.valueOf(this.startTime.longValue() * 1000L);
        }
      }
      Object endTimeVal = insensitiveMap.get("endTime");
      if (endTimeVal != null)
      {
        this.endTime = Long.valueOf(new Double(endTimeVal.toString()).longValue());
        if (this.endTime.longValue() < 100000000000L) {
          this.endTime = Long.valueOf(this.endTime.longValue() * 1000L);
        }
      }
      Map<String, Object> contextFilterVals = (Map)insensitiveMap.get("context");
      if (contextFilterVals != null)
      {
        this.contextFilter = new HashMap();
        for (Map.Entry<String, Object> entry : contextFilterVals.entrySet())
        {
          String className = "java.lang.String";
          if (!"$ANY$".equals(entry.getKey()))
          {
            for (Map.Entry<String, String> fieldEntry : contextType.fields.entrySet()) {
              if (((String)entry.getKey()).toLowerCase().equals(((String)fieldEntry.getKey()).toLowerCase())) {
                className = (String)fieldEntry.getValue();
              }
            }
            if (className == null) {
              throw new RuntimeException("Could not find field " + (String)entry.getKey() + " in type " + contextType.getFullName());
            }
          }
          Class<?> clazz = String.class;
          try
          {
            clazz = WALoader.get().loadClass(getBoxedClassName(className));
          }
          catch (ClassNotFoundException e)
          {
            throw new RuntimeException("Could not load class " + className + " for field " + (String)entry.getKey() + " in type " + contextType.getFullName());
          }
          FilterCondition fc = new FilterCondition(entry.getValue().toString(), clazz);
          this.contextFilter.put(entry.getKey(), fc);
        }
      }
      this.key = insensitiveMap.get("key");
      
      this.sortBy = ((String)insensitiveMap.get("sortBy"));
      
      String sortDir = (String)insensitiveMap.get("sortDir");
      if ((sortDir != null) && (sortDir.equalsIgnoreCase("asc"))) {
        this.sortDescending = false;
      }
      Object limitVal = insensitiveMap.get("limit");
      if (limitVal != null) {
        this.limit = Integer.parseInt(limitVal.toString());
      }
      Object singleWactionsVal = insensitiveMap.get("singleWactions");
      if ((singleWactionsVal != null) && ("true".equalsIgnoreCase(singleWactionsVal.toString()))) {
        this.getLatestPerKey = false;
      }
      if (!this.getLatestPerKey)
      {
        Object maxResultsVal = insensitiveMap.get("maxResults");
        if (maxResultsVal != null) {
          this.maxResults = Integer.valueOf(Integer.parseInt(maxResultsVal.toString()));
        }
        this.useMaxResults = (this.maxResults != null);
        this.groupBy = ((String)insensitiveMap.get("groupBy"));
      }
      for (String f : this.fields) {
        if (("eventList".equals(f)) || ("default-allEvents".equals(f))) {
          this.getEvents = true;
        }
      }
    }
  }
  
  public Map<String, Object> getFilterMap()
  {
    return this.filterMap;
  }
  
  public String[] getFields()
  {
    return this.fields;
  }
  
  public boolean willGetLatestPerKey()
  {
    return this.getLatestPerKey;
  }
  
  public boolean willGetEvents()
  {
    return this.getEvents;
  }
  
  public boolean requiresResultStats()
  {
    return this.useMaxResults;
  }
  
  private static Map makeCaseInsenstiveMap(Map<String, Object> sensitive)
  {
    Map<String, Object> insensitiveMap = NamePolicy.makeNameMap();
    for (Map.Entry<String, Object> entry : sensitive.entrySet()) {
      insensitiveMap.put(NamePolicy.makeKey((String)entry.getKey()), entry.getValue());
    }
    return insensitiveMap;
  }
  
  public class EventsComparator
    implements Comparator<SimpleEvent>
  {
    public EventsComparator() {}
    
    public int compare(SimpleEvent o1, SimpleEvent o2)
    {
      double n1 = 0.0D;
      double n2 = 0.0D;
      
      n1 = o1.timeStamp;
      n2 = o2.timeStamp;
      if (n1 > n2) {
        return 1;
      }
      if (n1 < n2) {
        return -1;
      }
      return o1._wa_SimpleEvent_ID.compareTo(o2._wa_SimpleEvent_ID);
    }
  }
  
  public static class ResultMapComparator
    implements Comparator<WactionKey>, Serializable
  {
    private static final long serialVersionUID = 3127965059367797612L;
    String mapField;
    transient Map<WactionKey, Map<String, Object>> base;
    boolean descending;
    
    public ResultMapComparator() {}
    
    public ResultMapComparator(String mapField, boolean descending)
    {
      this.mapField = mapField;
      this.descending = descending;
    }
    
    public int compare(WactionKey o1, WactionKey o2)
    {
      int c = this.descending ? -1 : 1;
      if (this.base == null) {
        return c * o1.compareTo(o2);
      }
      Map<String, Object> res1 = (Map)this.base.get(o1);
      Map<String, Object> res2 = (Map)this.base.get(o2);
      if ((res1 == null) && (res2 == null)) {
        return o1.compareTo(o2);
      }
      if ((res1 == null) && (res2 != null)) {
        return c;
      }
      if ((res1 != null) && (res2 == null)) {
        return -c;
      }
      Object ro1 = res1.get(this.mapField);
      Object ro2 = res2.get(this.mapField);
      if ((ro1 == null) && (ro2 == null)) {
        return o1.compareTo(o2);
      }
      if ((ro1 == null) && (ro2 != null)) {
        return c;
      }
      if ((ro1 != null) && (ro2 == null)) {
        return -c;
      }
      if (!ro1.getClass().isInstance(ro2)) {
        return o1.compareTo(o2);
      }
      if ((ro1 instanceof Number))
      {
        double d1 = ((Number)ro1).doubleValue();
        double d2 = ((Number)ro2).doubleValue();
        if (d1 > d2) {
          return c;
        }
        if (d2 > d1) {
          return -c;
        }
        return o1.compareTo(o2);
      }
      if ((ro1 instanceof DateTime))
      {
        DateTime d1 = (DateTime)ro1;
        DateTime d2 = (DateTime)ro2;
        
        int ct = d1.compareTo(d2) * c;
        return ct == 0 ? o1.compareTo(o2) : ct;
      }
      if ((ro1 instanceof String))
      {
        int ct = ((String)ro1).compareTo((String)ro2) * c;
        return ct == 0 ? o1.compareTo(o2) : ct;
      }
      String s1 = ro1.toString();
      String s2 = ro2.toString();
      
      int ct = s1.compareTo(s2);
      return ct == 0 ? o1.compareTo(o2) : ct;
    }
    
    public void setBaseMap(Map<WactionKey, Map<String, Object>> map)
    {
      this.base = map;
    }
    
    public String toString()
    {
      return "Map Comparator for field " + this.mapField + " desc " + this.descending + " base " + (this.base != null);
    }
  }
  
  private void adjustLatestPerKey(WactionKey wkey, Map<String, Object> data)
  {
    String key = wkey.key.toString();
    Map<String, Object> keyMap = (Map)this.unique.get(key);
    if (keyMap == null)
    {
      keyMap = new HashMap();
      this.unique.put(key, keyMap);
      this.mapper.put(key, wkey);
    }
    Integer keyNumEvents = (Integer)keyMap.get("totalEvents");
    if (keyNumEvents == null) {
      keyNumEvents = Integer.valueOf(0);
    }
    Integer currNumEvents = (Integer)data.get("totalEvents");
    if (currNumEvents != null) {
      keyNumEvents = Integer.valueOf(keyNumEvents.intValue() + currNumEvents.intValue());
    }
    keyMap.put("totalEvents", keyNumEvents);
    
    Long newestTS = (Long)keyMap.get("timestamp");
    if ((newestTS == null) || (((Long)data.get("timestamp")).longValue() > newestTS.longValue()))
    {
      for (Map.Entry<String, Object> ee : data.entrySet()) {
        if ((!((String)ee.getKey()).equals("eventList")) && (!((String)ee.getKey()).equals("totalEvents"))) {
          keyMap.put(ee.getKey(), ee.getValue());
        }
      }
      this.mapper.put(key, wkey);
    }
    List<SimpleEvent> keySortedEvents = (List)keyMap.get("eventList");
    if (keySortedEvents == null) {
      keySortedEvents = new ArrayList();
    }
    List<SimpleEvent> newEvents = (List)data.get("eventList");
    if (newEvents != null)
    {
      keySortedEvents.addAll(newEvents);
      keyMap.put("eventList", keySortedEvents);
    }
  }
  
  private void finalizeLatestPerKey()
  {
    EventsComparator ec = new EventsComparator();
    for (Map.Entry<Object, Map<String, Object>> ee : this.unique.entrySet())
    {
      List<SimpleEvent> events = (List)((Map)ee.getValue()).get("eventList");
      if (events != null) {
        Collections.sort(events, ec);
      }
      this.finalResults.put(this.mapper.get(ee.getKey()), ee.getValue());
    }
  }
  
  private void sort()
  {
    if (this.sortBy != null)
    {
      this.rmc = new ResultMapComparator(this.sortBy, this.sortDescending);
      this.rmc.setBaseMap(this.finalResults);
      Map<WactionKey, Map<String, Object>> sortedRes = new TreeMap(this.rmc);
      sortedRes.putAll(this.finalResults);
      this.finalResults = sortedRes;
    }
  }
  
  private void limit()
  {
    if (this.limit != -1)
    {
      Map<WactionKey, Map<String, Object>> limitMap;
      if (this.rmc != null) {
        limitMap = new TreeMap(this.rmc);
      } else {
        limitMap = new TreeMap();
      }
      int numPuts = 0;
      for (Map.Entry<WactionKey, Map<String, Object>> entry : this.finalResults.entrySet())
      {
        if (this.limit == numPuts) {
          break;
        }
        limitMap.put(entry.getKey(), entry.getValue());
        numPuts++;
      }
      this.finalResults = limitMap;
    }
  }
  
  private Collection<Waction> getWactions(Queryable<WactionKey, Waction> cache)
  {
    Map<WactionKey, Waction> wactions = null;
    if (this.singleKey != null)
    {
      if (logger.isDebugEnabled()) {
        logger.debug("--> Get Waction for Single Key: " + this.singleKey);
      }
      wactions = new HashMap();
      Waction w = (Waction)cache.get(this.singleKey);
      if (logger.isDebugEnabled()) {
        logger.debug("--> Got Waction for Single Key: " + w);
      }
      wactions.put(this.singleKey, w);
    }
    else if (this.key != null)
    {
      if (logger.isDebugEnabled()) {
        logger.debug("--> Get Wactions for key: " + this.key);
      }
      wactions = cache.getIndexedEqual("key", this.key);
      if (logger.isDebugEnabled()) {
        logger.debug("--> Got " + wactions.size() + " Wactions for key: " + this.key);
      }
    }
    else if ((this.startTime != null) || (this.endTime != null))
    {
      if (logger.isDebugEnabled()) {
        logger.debug("--> Get Wactions for range: " + this.startTime + " to " + this.endTime);
      }
      wactions = cache.getIndexedRange("ts", this.startTime, this.endTime);
      if (logger.isDebugEnabled()) {
        logger.debug("--> Got " + wactions.size() + " Wactions for range: " + this.startTime + " to " + this.endTime);
      }
    }
    if (wactions == null)
    {
      if (logger.isDebugEnabled()) {
        logger.debug("--> Get All Local Wactions");
      }
      wactions = cache.localEntries();
      if (logger.isDebugEnabled()) {
        logger.debug("--> Got " + wactions.size() + " Local Wactions");
      }
    }
    return wactions.values();
  }
  
  public WAQuery.ResultStats getResultStats(Iterable<Waction> wactions)
  {
    WAQuery.ResultStats stat = new WAQuery.ResultStats();
    for (Waction waction : wactions) {
      if (matches(waction))
      {
        if ((stat.startTime == 0L) || (waction.wactionTs < stat.startTime)) {
          stat.startTime = waction.wactionTs;
        }
        if ((stat.endTime == 0L) || (waction.wactionTs > stat.endTime)) {
          stat.endTime = waction.wactionTs;
        }
        stat.count += 1L;
      }
    }
    return stat;
  }
  
  public WAQuery.ResultStats getResultStats(Queryable<WactionKey, Waction> cache)
  {
    return getResultStats(getWactions(cache));
  }
  
  public void run(Queryable<WactionKey, Waction> cache)
  {
    Collection<Waction> wactions = getWactions(cache);
    if (wactions == null) {
      return;
    }
    for (Waction waction : wactions) {
      runOne(waction);
    }
  }
  
  public boolean matches(Waction waction)
  {
    long wactionTs = waction.wactionTs;
    if ((this.startTime != null) && (wactionTs < this.startTime.longValue())) {
      return false;
    }
    if ((this.endTime != null) && (wactionTs > this.endTime.longValue())) {
      return false;
    }
    if ((this.key != null) && (!this.key.equals(waction.key))) {
      return false;
    }
    if (this.contextFilter != null) {
      for (Map.Entry<String, FilterCondition> entry : this.contextFilter.entrySet())
      {
        if ("$ANY$".equals(entry.getKey()))
        {
          for (Map.Entry<String, Object> fieldEntry : waction.getContext().entrySet()) {
            if (((FilterCondition)entry.getValue()).evaluate(fieldEntry.getValue())) {
              return true;
            }
          }
          return false;
        }
        Object value = waction.getContext().get(entry.getKey());
        if (!((FilterCondition)entry.getValue()).evaluate(value)) {
          return false;
        }
      }
    }
    return true;
  }
  
  private Map<String, Object> getWactionAsMap(Waction waction)
  {
    Map<String, Object> result = new HashMap();
    result.put("timestamp", Long.valueOf(waction.getWactionTs()));
    result.put("uuid", waction.getUUIDString());
    result.put("key", waction.getKey());
    if (this.fields != null)
    {
      for (String s : this.fields) {
        if (waction.getContext().containsKey(s))
        {
          result.put("context-" + s, waction.getContext().get(s));
        }
        else if ("eventList".equals(s))
        {
          result.put(s, waction.getEvents());
        }
        else if ("status".equals(s))
        {
          result.put(s, Integer.valueOf(waction.getWactionStatus()));
        }
        else if ("default".equals(s))
        {
          for (Map.Entry<String, Object> entry : waction.getContext().entrySet()) {
            result.put("context-" + (String)entry.getKey(), entry.getValue());
          }
        }
        else if ("default-allEvents".equals(s))
        {
          for (Map.Entry<String, Object> entry : waction.getContext().entrySet()) {
            result.put("context-" + (String)entry.getKey(), entry.getValue());
          }
          result.put("eventList", waction.getEvents());
        }
      }
      result.put("totalEvents", Integer.valueOf(waction.numberOfEvents()));
    }
    return result;
  }
  
  public void setGlobalStats(WAQuery.ResultStats r)
  {
    this.globalStats = r;
    this.sampledResults = new HashMap();
    this.timePeriods = new TreeSet();
    this.doSampling = false;
    if ((this.globalStats.count != 0L) && 
      (this.useMaxResults) && (this.maxResults.intValue() < this.globalStats.count))
    {
      this.doSampling = true;
      long sampleTimePeriod = (this.globalStats.endTime - this.globalStats.startTime) / this.maxResults.intValue();
      if (sampleTimePeriod == 0L) {
        this.timePeriods.add(Long.valueOf(this.globalStats.endTime));
      } else {
        for (long t = this.globalStats.endTime; t > this.globalStats.startTime; t -= sampleTimePeriod) {
          this.timePeriods.add(Long.valueOf(t));
        }
      }
    }
  }
  
  private String getGroupByValue(Map<String, Object> data)
  {
    String groupByValue = null;
    if ((this.groupBy == null) || ("key".equalsIgnoreCase(this.groupBy))) {
      groupByValue = (String)data.get("key");
    } else {
      groupByValue = (String)data.get("context-" + this.groupBy);
    }
    if (groupByValue == null) {
      groupByValue = "<NOTSET>";
    }
    return groupByValue;
  }
  
  private static Set<String> dontMergeSet = new HashSet();
  
  static
  {
    dontMergeSet.add("count");
    dontMergeSet.add("timestamp");
    dontMergeSet.add("min");
    dontMergeSet.add("max");
    dontMergeSet.add("ave");
    dontMergeSet.add("sum");
    dontMergeSet.add("eventList");
  }
  
  private static enum StatsType
  {
    min,  max,  sum,  ave;
    
    private StatsType() {}
  }
  
  private Number sum(Number a, Number b)
  {
    if (b == null) {
      b = Integer.valueOf(0);
    }
    if ((a instanceof Byte))
    {
      Byte c = Byte.valueOf((byte)(a.byteValue() + b.byteValue()));
      return c;
    }
    if ((a instanceof Short))
    {
      Short c = Short.valueOf((short)(a.shortValue() + b.shortValue()));
      return c;
    }
    if ((a instanceof Integer))
    {
      Integer c = Integer.valueOf(a.intValue() + b.intValue());
      return c;
    }
    if ((a instanceof Long))
    {
      Long c = Long.valueOf(a.longValue() + b.longValue());
      return c;
    }
    if ((a instanceof Float))
    {
      Float c = Float.valueOf(a.floatValue() + b.floatValue());
      return c;
    }
    if ((a instanceof Double))
    {
      Double c = Double.valueOf(a.doubleValue() + b.doubleValue());
      return c;
    }
    return Integer.valueOf(0);
  }
  
  private Number ave(Number a, Long count)
  {
    if ((a instanceof Byte))
    {
      Byte c = Byte.valueOf((byte)(int)(a.byteValue() / count.longValue()));
      return c;
    }
    if ((a instanceof Short))
    {
      Short c = Short.valueOf((short)(int)(a.shortValue() / count.longValue()));
      return c;
    }
    if ((a instanceof Integer))
    {
      Integer c = Integer.valueOf((int)(a.intValue() / count.longValue()));
      return c;
    }
    if ((a instanceof Long))
    {
      Long c = Long.valueOf(a.longValue() / count.longValue());
      return c;
    }
    if ((a instanceof Float))
    {
      Float c = Float.valueOf(a.floatValue() / (float)count.longValue());
      return c;
    }
    if ((a instanceof Double))
    {
      Double c = Double.valueOf(a.doubleValue() / count.longValue());
      return c;
    }
    return Integer.valueOf(0);
  }
  
  private void createStatsData(Map<String, Object> groupByData, Map<String, Object> data, StatsType type)
  {
    Map<String, Object> groupByStatsData = (Map)groupByData.get(type.name());
    if (groupByStatsData == null)
    {
      groupByStatsData = new HashMap();
      groupByData.put(type.name(), groupByStatsData);
    }
    Map<String, Object> statsData = (Map)data.get(type.name());
    
    Iterator<Map.Entry<String, Object>> iterator = null;
    if (statsData != null) {
      iterator = statsData.entrySet().iterator();
    } else {
      iterator = data.entrySet().iterator();
    }
    while (iterator.hasNext())
    {
      Map.Entry<String, Object> dataEntry = (Map.Entry)iterator.next();
      if (!dontMergeSet.contains(dataEntry.getKey()))
      {
        Object currGroupByValue = groupByStatsData.get(dataEntry.getKey());
        Object currValue = dataEntry.getValue();
        if ((currValue instanceof Number)) {
          switch (type)
          {
          case min: 
            if ((currGroupByValue == null) || (((Comparable)currValue).compareTo(currGroupByValue) < 0)) {
              groupByStatsData.put(dataEntry.getKey(), currValue);
            }
            break;
          case max: 
            if ((currGroupByValue == null) || (((Comparable)currValue).compareTo(currGroupByValue) > 0)) {
              groupByStatsData.put(dataEntry.getKey(), currValue);
            }
            break;
          case sum: 
            groupByStatsData.put(dataEntry.getKey(), sum((Number)currValue, (Number)currGroupByValue));
            break;
          case ave: 
            Map<String, Object> groupBySumData = (Map)groupByData.get(StatsType.sum.name());
            Object sumValue = groupBySumData.get(dataEntry.getKey());
            Long count = (Long)groupByData.get("count");
            groupByStatsData.put(dataEntry.getKey(), ave((Number)sumValue, count));
          }
        }
      }
    }
  }
  
  private void mergeSampleData(Map<String, Object> groupByData, Map<String, Object> data)
  {
    Long count = (Long)groupByData.get("count");
    Long dataCount = (Long)data.get("count");
    if (dataCount == null) {
      dataCount = Long.valueOf(1L);
    }
    groupByData.put("count", Long.valueOf(count == null ? dataCount.longValue() : count.longValue() + dataCount.longValue()));
    
    Long dataTs = (Long)data.get("timestamp");
    Long groupByTs = (Long)groupByData.get("timestamp");
    if ((groupByTs == null) || (dataTs.longValue() > groupByTs.longValue()))
    {
      groupByData.put("timestamp", dataTs);
      for (Map.Entry<String, Object> dataEntry : data.entrySet()) {
        if (!dontMergeSet.contains(dataEntry.getKey())) {
          groupByData.put(dataEntry.getKey(), dataEntry.getValue());
        }
      }
    }
    createStatsData(groupByData, data, StatsType.min);
    createStatsData(groupByData, data, StatsType.max);
    createStatsData(groupByData, data, StatsType.sum);
    createStatsData(groupByData, data, StatsType.ave);
  }
  
  private void sampleResults(Map<String, Object> data)
  {
    String groupByValue = getGroupByValue(data);
    
    Map<Long, Map<String, Object>> groupByList = (Map)this.sampledResults.get(groupByValue);
    if (groupByList == null)
    {
      groupByList = new TreeMap();
      this.sampledResults.put(groupByValue, groupByList);
    }
    Long dataTs = (Long)data.get("timestamp");
    if (dataTs == null) {
      return;
    }
    Long sampleTimePeriod = (Long)this.timePeriods.ceiling(dataTs);
    if (sampleTimePeriod == null) {
      return;
    }
    Map<String, Object> groupByData = (Map)groupByList.get(sampleTimePeriod);
    if (groupByData == null)
    {
      groupByData = new HashMap();
      groupByList.put(sampleTimePeriod, groupByData);
    }
    mergeSampleData(groupByData, data);
  }
  
  private void finalizeSampleResults()
  {
    if (this.sampledResults == null) {
      return;
    }
    for (Map.Entry<String, Map<Long, Map<String, Object>>> sampleEntry : this.sampledResults.entrySet())
    {
      key = sampleEntry.getKey();
      for (Map.Entry<Long, Map<String, Object>> sampleKeyEntry : ((Map)sampleEntry.getValue()).entrySet())
      {
        UUID id = new UUID(((Long)sampleKeyEntry.getKey()).longValue());
        WactionKey wkey = new WactionKey(id, key);
        this.finalResults.put(wkey, sampleKeyEntry.getValue());
      }
    }
    Object key;
  }
  
  private boolean processResult(WactionKey wkey, Map<String, Object> data)
  {
    this.finalized = false;
    this.accepted += 1;
    if (this.getLatestPerKey)
    {
      adjustLatestPerKey(wkey, data);
    }
    else if (this.useMaxResults)
    {
      if (this.sampledResults == null)
      {
        if (this.globalStats == null) {
          return false;
        }
        setGlobalStats(this.globalStats);
      }
      if (!this.doSampling) {
        this.timePeriods.add((Long)data.get("timestamp"));
      }
      sampleResults(data);
    }
    else
    {
      this.finalResults.put(wkey, data);
      return true;
    }
    return false;
  }
  
  public int processed = 0;
  public int accepted = 0;
  
  public void runOne(Waction waction)
  {
    this.processed += 1;
    if (!matches(waction)) {
      return;
    }
    Map<String, Object> result = getWactionAsMap(waction);
    
    processResult(waction.getMapKey(), result);
  }
  
  public void mergeResults(Map<WactionKey, Map<String, Object>> results)
  {
    for (Map.Entry<WactionKey, Map<String, Object>> entry : results.entrySet()) {
      processResult((WactionKey)entry.getKey(), (Map)entry.getValue());
    }
  }
  
  boolean finalized = false;
  
  public Map<WactionKey, Map<String, Object>> getResults()
  {
    if (!this.finalized)
    {
      if ((this.getLatestPerKey) || (this.useMaxResults)) {
        this.finalResults.clear();
      }
      if (this.getLatestPerKey) {
        finalizeLatestPerKey();
      } else if (this.useMaxResults) {
        finalizeSampleResults();
      }
      sort();
      limit();
      this.finalized = true;
    }
    return this.finalResults;
  }
  
  public boolean usesSingleKey()
  {
    return this.singleKey != null;
  }
  
  public WactionKey getSingleKey()
  {
    return this.singleKey;
  }
  
  public void setSingleKey(WactionKey key)
  {
    this.singleKey = key;
  }
  
  public boolean usesPartitionKey()
  {
    return this.key != null;
  }
  
  public Object getPartitionKey()
  {
    return this.key;
  }
  
  public String toString()
  {
    String ret = "SELECT ";
    if ((this.fields == null) || (this.fields.length == 0))
    {
      ret = ret + "*";
    }
    else
    {
      boolean first = true;
      for (String f : this.fields)
      {
        if (!first) {
          ret = ret + ", ";
        }
        ret = ret + f;
        first = false;
      }
    }
    if (this.getLatestPerKey) {
      ret = ret + " LATEST";
    }
    ret = ret + " FROM " + this.wsName;
    ret = ret + " " + getWhere(null, false);
    if (this.sortBy != null) {
      ret = ret + " SORT BY " + this.sortBy + " " + (this.sortDescending ? "DESC" : "ASC");
    }
    if (this.limit != -1) {
      ret = ret + " LIMIT " + this.limit;
    }
    if (this.useMaxResults) {
      ret = ret + " SAMPLE TO " + this.maxResults;
    }
    return ret;
  }
  
  public String getWhere(String qualifier, boolean forNative)
  {
    String where = null;
    if (qualifier == null) {
      qualifier = "";
    } else {
      qualifier = qualifier + ".";
    }
    if (this.key != null) {
      if (forNative) {
        where = qualifier + "partitionKey = '" + this.key + "'";
      } else {
        where = qualifier + "key = '" + this.key + "'";
      }
    }
    if (this.startTime != null)
    {
      if (where != null) {
        where = where + " AND ";
      } else {
        where = "";
      }
      where = where + qualifier + "wactionTs >= " + this.startTime + "";
    }
    if (this.endTime != null)
    {
      if (where != null) {
        where = where + " AND ";
      } else {
        where = "";
      }
      where = where + qualifier + "wactionTs <= " + this.endTime + "";
    }
    boolean first;
    if ((this.contextFilter != null) && (!this.contextFilter.isEmpty()))
    {
      if (where != null) {
        where = where + " AND ";
      } else {
        where = "";
      }
      first = true;
      for (Map.Entry<String, FilterCondition> entry : this.contextFilter.entrySet())
      {
        if (!first) {
          where = where + " AND ";
        }
        where = where + qualifier + (String)entry.getKey() + " " + entry.getValue();
        first = false;
      }
    }
    return where != null ? "WHERE " + where : "";
  }
  
  public static void main(String[] args)
  {
    String[] conditions = { "$IN$one~two~three", "$IN$1~2~3", "$BTWN$10~20", "$BTWN$95000~96000", "$LIKE$abc", "$GT$10", "$GTE$10", "$LT$10", "$LTE$10", "$EQ$10", "$EQ$abc", "$NE$10", "$NE$10", "$NE$abc" };
    
    Class<?>[] classes = { String.class, Integer.class, Integer.class, String.class, String.class, Integer.class, Integer.class, Integer.class, Integer.class, Integer.class, String.class, Integer.class, String.class, String.class };
    
    Object[][] testValues = { { "one", "two", "four", Integer.valueOf(2) }, { Integer.valueOf(1), Integer.valueOf(2), Integer.valueOf(4), "two" }, { Integer.valueOf(1), Integer.valueOf(10), Integer.valueOf(15), Integer.valueOf(30), "forty" }, { "94086", "95051", "96000", "99000", Integer.valueOf(20) }, { "abc", "gabch", "GABCH", Integer.valueOf(4) }, { Integer.valueOf(1), Integer.valueOf(10), Integer.valueOf(20), "abc", "def" }, { Integer.valueOf(1), Integer.valueOf(10), Integer.valueOf(20), "abc", "def" }, { Integer.valueOf(1), Integer.valueOf(10), Integer.valueOf(20), "abc", "def" }, { Integer.valueOf(1), Integer.valueOf(10), Integer.valueOf(20), "abc", "def" }, { Integer.valueOf(1), Integer.valueOf(10), Integer.valueOf(20), "abc", "def" }, { Integer.valueOf(1), Integer.valueOf(10), Integer.valueOf(20), "abc", "def" }, { Integer.valueOf(1), Integer.valueOf(10), Integer.valueOf(20), "abc", "def" }, { Integer.valueOf(1), Integer.valueOf(10), Integer.valueOf(20), "10", "def" }, { Integer.valueOf(1), Integer.valueOf(10), Integer.valueOf(20), "abc", "def" } };
    for (int i = 0; i < conditions.length; i++)
    {
      String condition = conditions[i];
      Class<?> clazz = classes[i];
      try
      {
        FilterCondition fc = new FilterCondition(condition, clazz);
        
        Object[] objs = testValues[i];
        for (Object obj : objs) {
          try
          {
            System.out.println(fc + " on " + obj + " = " + fc.evaluate(obj));
          }
          catch (Exception e1)
          {
            System.out.println("* Could not run condition " + fc + " on " + obj + ": " + e1);
          }
        }
      }
      catch (Exception e)
      {
        System.out.println("* Could not create condition " + condition + ": " + e);
      }
    }
  }
  
  public List<Waction> executeQuery(Queryable<WactionKey, Waction> cache)
  {
    Collection<Waction> wactions = getWactions(cache);
    List<Waction> res = new ArrayList();
    addQueryResults(wactions, res);
    return res;
  }
  
  public void addQueryResults(Iterable<Waction> wactions, Collection<Waction> res)
  {
    if (wactions != null) {
      for (Waction waction : wactions)
      {
        this.processed += 1;
        if (matches(waction)) {
          res.add(waction);
        }
      }
    }
  }
  
  public List<Waction> getQueryResults()
  {
    return this.simpleQueryResults;
  }
  
  public void mergeQueryResults(List<Waction> results)
  {
    this.simpleQueryResults.addAll(results);
  }
  
  public WactionQuery() {}
}
