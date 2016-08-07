package com.bloom.runtime.components;

import com.bloom.exception.ESTypeCannotCastException;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.proc.events.DynamicEvent;
import com.bloom.runtime.containers.DynamicEventWrapper;
import com.bloom.uuid.UUID;
import com.bloom.waction.Waction;
import com.bloom.wactionstore.Utility;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.bloom.event.SimpleEvent;
import com.bloom.event.WactionConvertible;
import com.bloom.recovery.Position;
import com.bloom.runtime.RecordKey;
import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.ITaskEvent;
import com.bloom.runtime.containers.WAEvent;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

public abstract class CQSubTask
  implements Runnable
{
  private static final Logger logger = Logger.getLogger(CQSubTask.class);
  private static final RecordKey defaultKey = new RecordKey(new Object[0]);
  public CQTask context;
  private ITaskEvent taskEvent;
  private boolean flagIsAdd;
  private WAEvent curEvent;
  private IRemoveDuplicates dupRemover;
  private IGroupByPolicy grouper;
  private IOrderByPolicy sorter;
  private IOffsetAndLimit limiter;
  
  public abstract void setRow(WAEvent paramWAEvent);
  
  public abstract void init(CQTask paramCQTask);
  
  public abstract Object[] getSourceEvents();
  
  public abstract Object getRowData(int paramInt);
  
  public abstract Position getRowPosition(int paramInt);
  
  public abstract Object getWindowRow(int paramInt1, int paramInt2);
  
  public abstract int getThisDS();
  
  public abstract void runImpl();
  
  public abstract void updateState();
  
  public abstract void run();
  
  public final void setEvent(ITaskEvent event)
  {
    this.taskEvent = event;
  }
  
  public final boolean isAdd()
  {
    return this.flagIsAdd;
  }
  
  public final Object genNextInt()
  {
    return Integer.valueOf(this.sorter.genNextInt());
  }
  
  public final void setGroupByKeyAndAggVec(RecordKey groupByKey)
  {
    Object[] aggVec = this.context.getAggVec(groupByKey, isAdd());
    this.grouper.setGroupByKey(groupByKey, aggVec);
  }
  
  public final void setGroupByKeyAndDefaultAggVec()
  {
    setGroupByKeyAndAggVec(defaultKey);
  }
  
  public final Object getAggVec(int index)
  {
    return this.grouper.getAggVec(index);
  }
  
  public IBatch getAdded()
  {
    return this.taskEvent.batch();
  }
  
  public IBatch getAddedFiltered(RecordKey key)
  {
    return this.taskEvent.filterBatch(0, key);
  }
  
  public IBatch getRemoved()
  {
    return this.taskEvent.removedBatch();
  }
  
  private final void processBatch(IBatch batch)
  {
    IBatch<WAEvent> waEventIBatch = batch;
    for (WAEvent e : waEventIBatch)
    {
      setRow(e);
      try
      {
        runImpl();
      }
      catch (ESTypeCannotCastException e1)
      {
        if (logger.isInfoEnabled()) {
          logger.info(e1);
        }
      }
    }
  }
  
  private final void processAdded()
  {
    this.flagIsAdd = true;
    if ((atLeastOneDSisStreaming()) && 
      (isNotOuterJoin()) && 
      (!allStreamingSourcesReady())) {
      return;
    }
    processBatch(getAdded());
  }
  
  boolean atLeastOneDSisStreaming()
  {
    return this.context.atLeastOneDSisStreaming();
  }
  
  public boolean isNotOuterJoin()
  {
    return false;
  }
  
  boolean allStreamingSourcesReady()
  {
    return this.context.allStreamingSourcesReady();
  }
  
  private final void processRemoved()
  {
    this.flagIsAdd = false;
    processBatch(getRemoved());
  }
  
  public final void processAggregated()
  {
    Position batchLowPosition = buildLowPosition(getAdded());
    createResultBatch();
    processAdded();
    processRemoved();
    List<WAEvent> xnew = getResultBatch();
    for (WAEvent e : xnew) {
      e.position = (batchLowPosition == null ? null : new Position(batchLowPosition));
    }
    this.context.doOutput(xnew, Collections.emptyList());
  }
  
  public final void processNotAggregated()
  {
    createResultBatch();
    processAdded();
    List<WAEvent> xnew = getResultBatch();
    createResultBatch();
    processRemoved();
    List<WAEvent> xold = getResultBatch();
    for (WAEvent e : xnew) {
      if (e.position != null) {
        e.position = e.position.createAugmentedPosition(this.context.getMetaID(), null);
      }
    }
    this.context.doOutput(xnew, xold);
  }
  
  public final void setOutputEvent(Object data)
  {
    WAEvent event = new WAEvent(data, getRowPosition(getThisDS()));
    this.curEvent = event;
  }
  
  public final void removeGroupKey()
  {
    this.grouper.removeGroupKey();
  }
  
  public final void initResultBatchBuilder(IGroupByPolicy grouper, IOrderByPolicy sorter, IRemoveDuplicates dupRemover, IOffsetAndLimit limiter)
  {
    this.dupRemover = dupRemover;
    this.grouper = grouper;
    this.sorter = sorter;
    this.limiter = limiter;
  }
  
  private final void createResultBatch()
  {
    this.grouper.createBatch();
  }
  
  public final void setOrderByKey(RecordKey key)
  {
    this.sorter.setOrderByKey(key);
  }
  
  public final void linkSourceEvents()
  {
    if ((this.curEvent.data instanceof SimpleEvent))
    {
      SimpleEvent se = (SimpleEvent)this.curEvent.data;
      this.grouper.addSourceEvents(se, this, this.sorter);
    }
  }
  
  public final void addEvent()
  {
    Object eventWithKey = this.sorter.makeEventWithOrderByKey(this.curEvent);
    this.grouper.addEvent(eventWithKey);
  }
  
  private final List<WAEvent> getResultBatch()
  {
    Collection<Object> b = this.grouper.getResultBatch();
    Collection<Object> b2 = this.dupRemover.removeDuplicates(b);
    Collection<WAEvent> b3 = this.sorter.sort(b2);
    Collection<WAEvent> b4 = this.limiter.limitOutput(b3);
    List<WAEvent> res = makeList(b4);
    return res;
  }
  
  private Position buildLowPosition(Iterable<WAEvent> events)
  {
    if (!this.context.recoveryIsEnabled()) {
      return null;
    }
    Position result = null;
    for (WAEvent e : events) {
      if (e.position != null)
      {
        if (result == null) {
          result = new Position();
        }
        result.mergeLowerPositions(e.position);
      }
    }
    if (result != null) {
      result = result.createAugmentedPosition(this.context.getMetaID(), null);
    }
    return result;
  }
  
  public void caseNotFound()
  {
    throw new RuntimeException("Case not found");
  }
  
  public static boolean doLike(String s, Pattern p)
  {
    if ((s == null) || (p == null)) {
      return false;
    }
    Matcher m = p.matcher(s);
    return m.matches();
  }
  
  public Object getRowDataAndTranslate(Object obj, int ds)
    throws MetaDataRepositoryException
  {
    DynamicEvent e = (DynamicEvent)obj;
    TranslatedSchema schema = this.context.getTranslatedSchema(e.getEventType(), ds);
    return new DynamicEventWrapper(e, schema);
  }
  
  public Object getDynamicField(DynamicEventWrapper e, int fieldIndex, int dataSetIndex)
  {
    try
    {
      int translatedIndex = e.schema.realFieldIndex[fieldIndex];
      return translatedIndex < 0 ? null : e.event.data[translatedIndex];
    }
    catch (ArrayIndexOutOfBoundsException ex) {}
    return null;
  }
  
  public boolean checkDynamicRecType(DynamicEventWrapper e)
  {
    boolean isTypeExpected = e.schema.instanceOfExpectedType;
    return isTypeExpected;
  }
  
  private static Object castJsonTo(JsonNode fld, Class<?> type)
  {
    if (fld.isNull()) {
      return null;
    }
    if (Number.class.isAssignableFrom(type))
    {
      if (type == Integer.class) {
        return Integer.valueOf(fld.asInt());
      }
      if (type == Long.class) {
        return Long.valueOf(fld.asLong());
      }
      if (type == Short.class) {
        return Short.valueOf((short)fld.asInt());
      }
      if (type == Byte.class) {
        return Byte.valueOf((byte)fld.asInt());
      }
      if (type == Double.class) {
        return Double.valueOf(fld.asDouble());
      }
      if (type == Float.class) {
        return Float.valueOf((float)fld.asDouble());
      }
    }
    else
    {
      if (Date.class.isAssignableFrom(type))
      {
        String val = fld.asText();
        return Timestamp.valueOf(val);
      }
      if (DateTime.class.isAssignableFrom(type))
      {
        Long val = Long.valueOf(fld.asLong());
        return new DateTime(val.longValue());
      }
      if (type == Boolean.class) {
        return Boolean.valueOf(fld.asBoolean());
      }
      if (type == String.class) {
        return fld.asText();
      }
      if (type == UUID.class) {
        return new UUID(fld.textValue());
      }
      if (type.isAssignableFrom(JsonNode.class)) {
        return fld;
      }
      if (type == Object.class) {
        return fld;
      }
      if (type.getCanonicalName().equalsIgnoreCase("java.lang.Object[]"))
      {
        ArrayNode arrayNode = (ArrayNode)fld;
        Object[] resultArray = new Object[arrayNode.size()];
        Iterator<JsonNode> iterator = arrayNode.iterator();
        int i = 0;
        while (iterator.hasNext()) {
          resultArray[(i++)] = ((JsonNode)iterator.next()).asText();
        }
        return resultArray;
      }
      if (type.getCanonicalName().equalsIgnoreCase("java.util.HashMap"))
      {
        Map<String, Object> result = (Map)Utility.objectMapper.convertValue(fld, Map.class);
        return result;
      }
      if (type.getCanonicalName().equalsIgnoreCase("byte[]"))
      {
        byte[] result = (byte[])Utility.objectMapper.convertValue(fld, byte[].class);
        return result;
      }
    }
    throw new RuntimeException("cannot convert json object " + fld + " to object of type " + type.getCanonicalName());
  }
  
  public static Object dynamicCast(JsonNode obj, String fieldName, Class<?> type)
  {
    ObjectNode waction = (ObjectNode)obj;
    JsonNode fld = waction.get(fieldName);
    if (fld == null) {
      throw new ESTypeCannotCastException("cannot access field <" + fieldName + "> in event " + obj);
    }
    return castJsonTo(fld, type);
  }
  
  public CQTask getContext()
  {
    return this.context;
  }
  
  public Object convertWA2Ctx(Object o, Class<?> klazz)
  {
    Waction wa = (Waction)o;
    try
    {
      SimpleEvent ev = (SimpleEvent)klazz.newInstance();
      if (((ev instanceof WactionConvertible)) && (wa != null)) {
        ((WactionConvertible)ev).convertFromWactionToEvent(wa.getWactionTs(), wa.getUuid(), wa.getKeyString(), wa.getContext());
      }
      return ev;
    }
    catch (InstantiationException|IllegalAccessException e)
    {
      throw new RuntimeException(e);
    }
  }
  
  public Object getWARowContext(Object obj, Class<?> klazz)
  {
    return convertWA2Ctx(obj, klazz);
  }
  
  private static String toText(Object o)
  {
    return o.toString();
  }
  
  private static Number toNum(Object o)
  {
    return (o instanceof Number) ? (Number)o : new BigDecimal(toText(o));
  }
  
  private static Boolean toBool(Object o)
  {
    return (o instanceof Boolean) ? (Boolean)o : Boolean.valueOf(toText(o));
  }
  
  public Object getParamVal(int index, String pname, Class<?> expectedType)
  {
    Object val = this.context.getParam(index);
    if (val == null) {
      return null;
    }
    if (expectedType == Object.class) {
      return val;
    }
    if (Number.class.isAssignableFrom(expectedType))
    {
      if (expectedType == Integer.class) {
        return Integer.valueOf(toNum(val).intValue());
      }
      if (expectedType == Long.class) {
        return Long.valueOf(toNum(val).longValue());
      }
      if (expectedType == Short.class) {
        return Short.valueOf(toNum(val).shortValue());
      }
      if (expectedType == Byte.class) {
        return Byte.valueOf(toNum(val).byteValue());
      }
      if (expectedType == Double.class) {
        return Double.valueOf(toNum(val).doubleValue());
      }
      if (expectedType == Float.class) {
        return Float.valueOf(toNum(val).floatValue());
      }
    }
    else
    {
      if (Date.class.isAssignableFrom(expectedType))
      {
        String tmp = toText(val);
        return Timestamp.valueOf(tmp);
      }
      if (DateTime.class.isAssignableFrom(expectedType))
      {
        Long tmp = Long.valueOf(toNum(val).longValue());
        return new DateTime(tmp.longValue());
      }
      if (expectedType == Boolean.class) {
        return toBool(val);
      }
      if (expectedType == String.class) {
        return toText(val);
      }
      if (expectedType == UUID.class) {
        return new UUID(toText(val));
      }
    }
    throw new RuntimeException("cannot convert parameter " + pname + " to type " + expectedType.getCanonicalName());
  }
  
  public static Object getJavaObjectField(Object obj, String fieldName)
  {
    try
    {
      Class<?> objType = obj.getClass();
      Field fld = objType.getField(fieldName);
      return fld.get(obj);
    }
    catch (NoSuchFieldException|SecurityException|IllegalArgumentException|IllegalAccessException e) {}
    return null;
  }
  
  public static Object getJsonObjectField(JsonNode obj, String fieldName, Class<?> type)
  {
    JsonNode fldVal = obj.get(fieldName);
    return fldVal != null ? castJsonTo(fldVal, type) : null;
  }
  
  private static <T> List<T> makeList(Collection<T> col)
  {
    List<T> ret;
    List<T> ret;
    if ((col instanceof List)) {
      ret = (List)col;
    } else {
      ret = new ArrayList(col);
    }
    return ret;
  }
  
  public static IGroupByPolicy makeGrouped()
  {
    return new Grouped(null);
  }
  
  public static IGroupByPolicy makeNotGrouped()
  {
    return new NotGrouped(null);
  }
  
  public static IOrderByPolicy makeOrdered(boolean sortasc)
  {
    return new Ordered(sortasc);
  }
  
  public static IOrderByPolicy makeNotOrdered()
  {
    return notOrdered;
  }
  
  public static IRemoveDuplicates makeRemoveDups()
  {
    return doRemove;
  }
  
  public static IRemoveDuplicates makeNotRemoveDups()
  {
    return doNotRemove;
  }
  
  public static IOffsetAndLimit makeLimited(int offset, int limit)
  {
    return new HasOffsetAndLimit(offset, limit);
  }
  
  public static IOffsetAndLimit makeNotLimited()
  {
    return noLimits;
  }
  
  private static final IOrderByPolicy notOrdered = new IOrderByPolicy()
  {
    public void setOrderByKey(RecordKey key)
    {
      throw new UnsupportedOperationException();
    }
    
    public int genNextInt()
    {
      throw new UnsupportedOperationException();
    }
    
    public Object makeEventWithOrderByKey(WAEvent event)
    {
      return event;
    }
    
    public WAEvent getEventWithoutKey(Object o)
    {
      return (WAEvent)o;
    }
    
    public Collection<WAEvent> sort(Collection<?> col)
    {
      return col;
    }
    
    public String toString()
    {
      return "not ordered";
    }
  };
  
  public abstract void cleanState();
  
  private static abstract interface IOrderByPolicy
  {
    public abstract void setOrderByKey(RecordKey paramRecordKey);
    
    public abstract Object makeEventWithOrderByKey(WAEvent paramWAEvent);
    
    public abstract WAEvent getEventWithoutKey(Object paramObject);
    
    public abstract Collection<WAEvent> sort(Collection<?> paramCollection);
    
    public abstract int genNextInt();
  }
  
  private static class Ordered
    implements CQSubTask.IOrderByPolicy
  {
    private static final Comparator<RecordKey> comparator = new Comparator()
    {
      public int compare(RecordKey k1, RecordKey k2)
      {
        return k1.compareTo(k2);
      }
    };
    private static final Comparator<RecordKey> reverse_comparator = Collections.reverseOrder(comparator);
    private int nextInt;
    private RecordKey orderByKey;
    private final boolean sortasc;
    
    Ordered(boolean sortasc)
    {
      this.sortasc = sortasc;
    }
    
    public void setOrderByKey(RecordKey key)
    {
      this.orderByKey = key;
    }
    
    public int genNextInt()
    {
      return this.nextInt++;
    }
    
    public Object makeEventWithOrderByKey(WAEvent event)
    {
      return new CQSubTask.OrderKeyAndEvent(this.orderByKey, event);
    }
    
    public WAEvent getEventWithoutKey(Object o)
    {
      CQSubTask.OrderKeyAndEvent p = (CQSubTask.OrderKeyAndEvent)o;
      return p == null ? null : p.event;
    }
    
    public Collection<WAEvent> sort(Collection<?> col)
    {
      return sortList(col, this.sortasc ? comparator : reverse_comparator);
    }
    
    private Collection<WAEvent> sortList(Collection<CQSubTask.OrderKeyAndEvent> col, Comparator<RecordKey> cmp)
    {
      Map<RecordKey, WAEvent> index = new TreeMap(cmp);
      for (CQSubTask.OrderKeyAndEvent e : col) {
        index.put(e.key, e.event);
      }
      Collection<WAEvent> ret = index.values();
      return ret;
    }
    
    public String toString()
    {
      return "ordered " + (this.sortasc ? "asc" : "desc");
    }
  }
  
  private static abstract interface IGroupByPolicy
  {
    public abstract void setGroupByKey(RecordKey paramRecordKey, Object[] paramArrayOfObject);
    
    public abstract void removeGroupKey();
    
    public abstract void addSourceEvents(SimpleEvent paramSimpleEvent, CQSubTask paramCQSubTask, CQSubTask.IOrderByPolicy paramIOrderByPolicy);
    
    public abstract void addEvent(Object paramObject);
    
    public abstract void createBatch();
    
    public abstract Collection<Object> getResultBatch();
    
    public abstract Object getAggVec(int paramInt);
  }
  
  private static class NotGrouped
    implements CQSubTask.IGroupByPolicy
  {
    private List<Object> batch;
    
    public void addSourceEvents(SimpleEvent se, CQSubTask owner, CQSubTask.IOrderByPolicy sorter)
    {
      if (owner.isAdd())
      {
        List<Object[]> lst = Collections.singletonList(owner.getSourceEvents());
        
        se.setSourceEvents(lst);
      }
    }
    
    public void setGroupByKey(RecordKey key, Object[] aggVec)
    {
      throw new UnsupportedOperationException();
    }
    
    public Object getAggVec(int index)
    {
      throw new UnsupportedOperationException();
    }
    
    public void removeGroupKey()
    {
      throw new UnsupportedOperationException();
    }
    
    public void addEvent(Object o)
    {
      this.batch.add(o);
    }
    
    public void createBatch()
    {
      this.batch = new ArrayList();
    }
    
    public Collection<Object> getResultBatch()
    {
      return this.batch;
    }
    
    public String toString()
    {
      return "not grouped";
    }
  }
  
  private static class Grouped
    implements CQSubTask.IGroupByPolicy
  {
    private RecordKey groupByKey;
    private Object[] aggVec;
    private Map<Object, Object> batch;
    
    public void addSourceEvents(SimpleEvent se, CQSubTask owner, CQSubTask.IOrderByPolicy sorter)
    {
      List<Object[]> lst = null;
      WAEvent wae = sorter.getEventWithoutKey(this.batch.get(this.groupByKey));
      if (wae != null)
      {
        SimpleEvent pe = (SimpleEvent)wae.data;
        lst = pe.linkedSourceEvents;
      }
      if (owner.isAdd())
      {
        if (lst == null) {
          lst = new ArrayList(2);
        }
        lst.add(owner.getSourceEvents());
      }
      se.setSourceEvents(lst);
    }
    
    public void setGroupByKey(RecordKey key, Object[] aggVec)
    {
      this.groupByKey = key;
      this.aggVec = ((Object[])aggVec.clone());
    }
    
    public Object getAggVec(int index)
    {
      return this.aggVec[index];
    }
    
    public void removeGroupKey()
    {
      this.batch.remove(this.groupByKey);
    }
    
    public void addEvent(Object o)
    {
      this.batch.put(this.groupByKey, o);
    }
    
    public void createBatch()
    {
      this.batch = new LinkedHashMap();
    }
    
    public Collection<Object> getResultBatch()
    {
      return this.batch.values();
    }
    
    public String toString()
    {
      return "grouped";
    }
  }
  
  private static final IRemoveDuplicates doRemove = new IRemoveDuplicates()
  {
    public Collection<Object> removeDuplicates(Collection<Object> col)
    {
      return new LinkedHashSet(col);
    }
    
    public String toString()
    {
      return "remove duplicates";
    }
  };
  private static final IRemoveDuplicates doNotRemove = new IRemoveDuplicates()
  {
    public Collection<Object> removeDuplicates(Collection<Object> col)
    {
      return col;
    }
    
    public String toString()
    {
      return "do not remove duplicates";
    }
  };
  private static final IOffsetAndLimit noLimits = new IOffsetAndLimit()
  {
    public Collection<WAEvent> limitOutput(Collection<WAEvent> col)
    {
      return col;
    }
    
    public String toString()
    {
      return "not limited";
    }
  };
  
  private static abstract interface IRemoveDuplicates
  {
    public abstract Collection<Object> removeDuplicates(Collection<Object> paramCollection);
  }
  
  private static abstract interface IOffsetAndLimit
  {
    public abstract Collection<WAEvent> limitOutput(Collection<WAEvent> paramCollection);
  }
  
  private static class HasOffsetAndLimit
    implements CQSubTask.IOffsetAndLimit
  {
    private final int offset;
    private final int limit;
    
    HasOffsetAndLimit(int offset, int limit)
    {
      this.offset = offset;
      this.limit = limit;
    }
    
    public Collection<WAEvent> limitOutput(Collection<WAEvent> col)
    {
      int size = col.size();
      if (this.offset == 0)
      {
        if (size <= this.limit) {
          return col;
        }
        return getSubList(col, 0, this.limit);
      }
      if (this.offset >= size) {
        return Collections.emptyList();
      }
      int lim = this.offset + this.limit;
      if (size <= lim) {
        return getSubList(col, this.offset, size);
      }
      return getSubList(col, this.offset, lim);
    }
    
    private static Collection<WAEvent> getSubList(Collection<WAEvent> col, int begin, int end)
    {
      if ((col instanceof List)) {
        return ((List)col).subList(begin, end);
      }
      List<WAEvent> res = new ArrayList();
      int i = 0;
      for (WAEvent e : col)
      {
        if (i >= begin)
        {
          if (i >= end) {
            break;
          }
          res.add(e);
        }
        i++;
      }
      return res;
    }
    
    public String toString()
    {
      return "limit " + this.limit + " offset " + this.offset;
    }
  }
  
  private static class OrderKeyAndEvent
  {
    public final RecordKey key;
    public final WAEvent event;
    
    public OrderKeyAndEvent(RecordKey key, WAEvent event)
    {
      this.key = key;
      this.event = event;
    }
    
    public final boolean equals(Object o)
    {
      if (!(o instanceof OrderKeyAndEvent)) {
        return false;
      }
      OrderKeyAndEvent other = (OrderKeyAndEvent)o;
      return this.event.equals(other.event);
    }
    
    public final int hashCode()
    {
      return this.event.hashCode();
    }
    
    public final String toString()
    {
      return "(" + this.key + "," + this.event + ")";
    }
  }
}
