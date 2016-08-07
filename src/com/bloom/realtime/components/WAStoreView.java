package com.bloom.runtime.components;

import com.fasterxml.jackson.databind.JsonNode;
import com.bloom.classloading.WALoader;
import com.bloom.event.SimpleEvent;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.persistence.WactionStore;
import com.bloom.proc.events.JsonNodeEvent;
import com.bloom.runtime.BaseServer;
import com.bloom.runtime.RecordKey;
import com.bloom.runtime.channels.Channel;
import com.bloom.runtime.compiler.select.AST2JSON;
import com.bloom.runtime.containers.Batch;
import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.ITaskEvent;
import com.bloom.runtime.containers.Range;
import com.bloom.runtime.containers.TaskEvent;
import com.bloom.runtime.containers.WAEvent;
import com.bloom.runtime.meta.IntervalPolicy;
import com.bloom.runtime.meta.IntervalPolicy.TimeBasedPolicy;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.Type;
import com.bloom.runtime.meta.MetaInfo.WAStoreView;
import com.bloom.runtime.meta.MetaInfo.WActionStore;
import com.bloom.runtime.meta.MetaInfoStatus;
import com.bloom.runtime.monitor.MonitorEvent.Type;
import com.bloom.runtime.monitor.MonitorEventsCollection;
import com.bloom.waction.Waction;
import com.bloom.waction.WactionKey;
import com.bloom.wactionstore.WAction;
import com.bloom.wactionstore.WActionQuery;
import com.bloom.wactionstore.WActionStoreManager;
import com.bloom.wactionstore.WActionStores;
import java.lang.reflect.Field;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

public class WAStoreView
  extends IWindow
  implements Runnable
{
  private static Logger logger = Logger.getLogger(WAStoreView.class);
  private final WactionStore owner;
  private final MetaInfo.WAStoreView viewMetaInfo;
  private final Channel output;
  private final long timerLimit;
  private volatile boolean running = false;
  private Publisher dataSource;
  private long startTime;
  private long endTime;
  private ScheduledFuture<?> jumpingTask;
  private WActionStoreManager cachedManager;
  private long inputTotal = 0L;
  private Long prevInputTotal = null;
  private Long prevInputRate = null;
  
  public WAStoreView(MetaInfo.WAStoreView viewMetaInfo, WactionStore owner, BaseServer srv)
  {
    super(srv, viewMetaInfo);
    this.owner = owner;
    this.viewMetaInfo = viewMetaInfo;
    IntervalPolicy viewIntervalPolicy = viewMetaInfo.viewSize;
    if ((viewIntervalPolicy != null) && (viewIntervalPolicy.isTimeBased())) {
      this.timerLimit = viewIntervalPolicy.getTimePolicy().getTimeInterval();
    } else {
      this.timerLimit = 0L;
    }
    this.output = srv.createChannel(this);
    this.output.addCallback(this);
  }
  
  public Channel getChannel()
  {
    return this.output;
  }
  
  public void close()
    throws Exception
  {
    stop();
    this.output.close();
  }
  
  public boolean isRunning()
  {
    return this.running;
  }
  
  public void connectParts(Flow flow)
    throws Exception
  {
    this.dataSource = flow.getWactionStore(this.viewMetaInfo.wastoreID);
  }
  
  public String toString()
  {
    return getMetaUri() + " - " + getMetaID() + " RUNTIME";
  }
  
  public void receive(Object linkID, ITaskEvent event)
    throws Exception
  {
    this.output.publish(event);
  }
  
  public void start()
    throws Exception
  {
    if (this.running) {
      return;
    }
    this.running = true;
    if (this.viewMetaInfo.subscribeToUpdates) {
      srv().subscribe(this.dataSource, this);
    }
    if ((this.timerLimit > 0L) && (this.viewMetaInfo.isJumping.booleanValue()))
    {
      this.startTime = System.currentTimeMillis();
      this.jumpingTask = srv().getScheduler().scheduleAtFixedRate(this, this.timerLimit, this.timerLimit, TimeUnit.MICROSECONDS);
    }
  }
  
  public void stop()
    throws Exception
  {
    if (!this.running) {
      return;
    }
    resetProcessThread();
    this.running = false;
    if (this.viewMetaInfo.subscribeToUpdates) {
      srv().unsubscribe(this.dataSource, this);
    }
    if (this.jumpingTask != null)
    {
      this.jumpingTask.cancel(true);
      this.jumpingTask = null;
    }
  }
  
  public void notifyMe(Link link)
  {
    try
    {
      Range r;
      Range r;
      if (this.timerLimit == 0L)
      {
        r = queryAllStore();
      }
      else
      {
        Range r;
        if (!this.viewMetaInfo.isJumping.booleanValue())
        {
          long end = System.currentTimeMillis();
          long begin = end - TimeUnit.MICROSECONDS.toMillis(this.timerLimit);
          r = queryRange(begin, end);
        }
        else
        {
          r = Range.emptyRange();
        }
      }
      TaskEvent snapshotEvent = TaskEvent.createWindowStateEvent(r);
      link.subscriber.receive(Integer.valueOf(link.linkID), snapshotEvent);
    }
    catch (Exception e)
    {
      logger.error(e);
      if ((getFlow() != null) && (getFlow().getMetaInfo() != null) && (getFlow().getMetaInfo().getMetaInfoStatus().isAdhoc())) {
        throw new RuntimeException(e.getMessage());
      }
    }
  }
  
  public void run()
  {
    this.endTime = System.currentTimeMillis();
    try
    {
      Range r = queryRange(this.startTime, this.endTime);
      TaskEvent snapshotEvent = TaskEvent.createWAStoreQueryEvent(r);
      this.output.publish(snapshotEvent);
    }
    catch (Exception e)
    {
      logger.warn(e.getMessage());
    }
    this.startTime = (this.endTime + 1L);
  }
  
  public void addSpecificMonitorEvents(MonitorEventsCollection monEvs)
  {
    Long it = Long.valueOf(this.inputTotal);
    
    long timeStamp = monEvs.getTimeStamp();
    if (!it.equals(this.prevInputTotal)) {
      monEvs.add(MonitorEvent.Type.INPUT, it);
    }
    if (this.prevTimeStamp != null)
    {
      Long ir = Long.valueOf(1000L * (it.longValue() - this.prevInputTotal.longValue()) / (timeStamp - this.prevTimeStamp.longValue()));
      if (!ir.equals(this.prevInputRate))
      {
        monEvs.add(MonitorEvent.Type.RATE, ir);
        monEvs.add(MonitorEvent.Type.INPUT_RATE, ir);
        this.prevInputRate = ir;
      }
    }
    this.prevInputTotal = it;
  }
  
  private Range executeQueryOldStyle(final long startTime, long endTime)
  {
    new Range()
    {
      private static final long serialVersionUID = -4805866185978952939L;
      private transient List<WAEvent> values;
      
      public Batch all()
      {
        if (this.values == null) {
          this.values = WAStoreView.this.fetchWactions(startTime, this.val$endTime);
        }
        return Batch.asBatch(this.values);
      }
      
      public String toString()
      {
        return "(wastore view range for time : " + new Date(startTime) + "-" + new Date(this.val$endTime) + ")";
      }
    };
  }
  
  public synchronized List<WAEvent> fetchWactions(long startTime, long endTime)
  {
    final Collection<Waction> res = this.owner.getWactions(startTime, endTime);
    new AbstractList()
    {
      public Iterator<WAEvent> iterator()
      {
        new Iterator()
        {
          Iterator<Waction> it = WAStoreView.2.this.val$res.iterator();
          
          public boolean hasNext()
          {
            return this.it.hasNext();
          }
          
          public WAEvent next()
          {
            return new WAEvent(this.it.next());
          }
          
          public void remove()
          {
            throw new UnsupportedOperationException();
          }
        };
      }
      
      public WAEvent get(int index)
      {
        throw new UnsupportedOperationException();
      }
      
      public int size()
      {
        return res.size();
      }
    };
  }
  
  private Class<?> getContextClass()
  {
    try
    {
      return WALoader.get().loadClass(this.owner.contextBeanDef.className);
    }
    catch (ClassNotFoundException e)
    {
      logger.warn(e.getMessage());
      throw new RuntimeException(e);
    }
  }
  
  private SimpleEvent createContextEvent(Class<?> clazz)
  {
    try
    {
      return (SimpleEvent)clazz.newInstance();
    }
    catch (InstantiationException|IllegalAccessException e)
    {
      logger.warn(e.getMessage());
      throw new RuntimeException(e);
    }
  }
  
  private void setField(SimpleEvent ev, Field fld, String fieldName, Map<String, Object> vals)
  {
    if (vals.containsKey(fieldName)) {
      try
      {
        Object fieldVal = vals.get(fieldName);
        fld.set(ev, fieldVal);
      }
      catch (IllegalArgumentException|IllegalAccessException e)
      {
        logger.warn(e.getMessage());
        throw new RuntimeException(e);
      }
    }
  }
  
  private void setContextEventFieldsFromMap(SimpleEvent ev, Field[] fields, Map<String, Object> vals)
  {
    if (ev.setFromContextMap(vals)) {
      return;
    }
    ev.timeStamp = ((Long)vals.get("timestamp")).longValue();
    for (Field fld : fields)
    {
      String fieldName = "context-" + fld.getName();
      setField(ev, fld, fieldName, vals);
    }
  }
  
  private synchronized List<WAEvent> loadValues(long startTime, long endTime)
  {
    Map<String, Object> settingsForFilter = new HashMap();
    settingsForFilter.put("singleWactions", "True");
    settingsForFilter.put("startTime", Long.valueOf(startTime));
    settingsForFilter.put("endTime", Long.valueOf(endTime));
    Map<WactionKey, Map<String, Object>> allWactions = this.owner.getWactions(null, null, settingsForFilter);
    Class<?> clazz = getContextClass();
    List<WAEvent> resultSet = new ArrayList();
    if (allWactions != null) {
      for (Map<String, Object> waction : allWactions.values())
      {
        SimpleEvent ev = createContextEvent(clazz);
        setContextEventFieldsFromMap(ev, clazz.getFields(), waction);
        resultSet.add(new WAEvent(ev));
      }
    }
    return resultSet;
  }
  
  private Range queryRange(long startTime, long endTime)
    throws MetaDataRepositoryException
  {
    if (this.viewMetaInfo.hasQuery())
    {
      JsonNode jsonQuery = getQuery();
      AST2JSON.addTimeBounds(jsonQuery, startTime, endTime);
      return executeQuery(jsonQuery);
    }
    return executeQueryOldStyle(startTime, endTime);
  }
  
  private Range queryAllStore()
    throws MetaDataRepositoryException
  {
    if (this.viewMetaInfo.hasQuery()) {
      return executeQuery(getQuery());
    }
    return executeQueryOldStyle(0L, System.currentTimeMillis());
  }
  
  private WAEvent convert(WAction a)
  {
    JsonNodeEvent e = new JsonNodeEvent();
    e.setData(a);
    return new WAEvent(e);
  }
  
  private Range makeSnapshot(final Iterable<WAction> rs)
  {
    new Range()
    {
      private static final long serialVersionUID = 8193566130544575344L;
      
      public Batch all()
      {
        new Batch()
        {
          private static final long serialVersionUID = -6673129930236624672L;
          
          public Iterator<WAEvent> iterator()
          {
            final Iterator<WAction> it = WAStoreView.3.this.val$rs.iterator();
            new Iterator()
            {
              public boolean hasNext()
              {
                return it.hasNext();
              }
              
              public WAEvent next()
              {
                return WAStoreView.this.convert((WAction)it.next());
              }
              
              public void remove()
              {
                throw new UnsupportedOperationException();
              }
            };
          }
          
          public int size()
          {
            int count = 0;
            for (WAction a : WAStoreView.3.this.val$rs) {
              count++;
            }
            return count;
          }
          
          public boolean isEmpty()
          {
            return !iterator().hasNext();
          }
        };
      }
      
      public IBatch lookup(int indexID, RecordKey key)
      {
        return Batch.emptyBatch();
      }
      
      public String toString()
      {
        return "(wastore view iterator range :" + rs + ")";
      }
    };
  }
  
  private JsonNode getQuery()
  {
    JsonNode jsonQuery = AST2JSON.deserialize(this.viewMetaInfo.query);
    return jsonQuery;
  }
  
  private Range executeQuery(JsonNode jsonQuery)
    throws MetaDataRepositoryException
  {
    if (this.cachedManager == null)
    {
      MetaInfo.WActionStore ws = (MetaInfo.WActionStore)srv().getObject(this.viewMetaInfo.wastoreID);
      this.cachedManager = WActionStores.getInstance(ws.properties);
    }
    WActionQuery q = this.cachedManager.prepareQuery(jsonQuery);
    return makeSnapshot(q.execute());
  }
}
