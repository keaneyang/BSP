package com.bloom.runtime.components;

import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.distribution.WAQueue;
import com.bloom.proc.events.ShowStreamEvent;
import com.bloom.runtime.BaseServer;
import com.bloom.runtime.Interval;
import com.bloom.runtime.KeyFactory;
import com.bloom.runtime.StreamEventFactory;
import com.bloom.runtime.channels.Channel;
import com.bloom.runtime.channels.DistributedChannel;
import com.bloom.runtime.channels.KafkaChannel;
import com.bloom.runtime.channels.ZMQChannel;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.ShowStream;
import com.bloom.runtime.meta.MetaInfo.Stream;
import com.bloom.runtime.monitor.MonitorEventsCollection;
import com.bloom.runtime.monitor.MonitorEvent.Type;
import com.bloom.uuid.UUID;
import com.bloom.recovery.PartitionedSourcePosition;
import com.bloom.recovery.Position;
import com.bloom.runtime.RecordKey;
import com.bloom.runtime.containers.ITaskEvent;
import com.bloom.runtime.containers.WAEvent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Logger;

@PropertyTemplate(name="KafkaStream", type=AdapterType.stream, properties={@com.bloom.anno.PropertyTemplateProperty(name="zk.address", type=String.class, required=true, defaultValue="localhost:2181"), @com.bloom.anno.PropertyTemplateProperty(name="bootstrap.brokers", type=String.class, required=true, defaultValue="localhost:9092"), @com.bloom.anno.PropertyTemplateProperty(name="partitions", type=Integer.class, required=false, defaultValue="localhost:9092"), @com.bloom.anno.PropertyTemplateProperty(name="dataformat", type=String.class, required=false, defaultValue="")})
public class Stream
  extends FlowComponent
  implements PubSub, Runnable
{
  private static Logger logger = Logger.getLogger(Stream.class);
  private static Map<String, Integer> allTungstenShowStreams = new ConcurrentHashMap();
  private final MetaInfo.Stream streamInfo;
  private DistributedChannel output;
  private final Map<UUID, Flow> connectedFlows = new HashMap();
  private volatile long inputTotal = 0L;
  private final boolean hasDelayBuffer;
  private KeyFactory keyFactory;
  private ConcurrentSkipListMap<Key, Object> index;
  private AtomicLong nextGen = new AtomicLong(1L);
  private long interval;
  private Future<?> task;
  private RecordKey boundKey;
  private Map<String, ActiveShowStream> activeShows = new HashMap();
  private boolean isRecoveryEnabled = false;
  Long prevInputTotal = null;
  Long prevInputRate = null;
  Boolean prevFull = null;
  
  private class ActiveShowStream
  {
    public WAQueue consoleQueue = null;
    public AtomicInteger limit_queue = null;
    
    private ActiveShowStream() {}
  }
  
  private static class Key
  {
    final RecordKey key;
    final long id;
    long timestamp;
    
    Key(RecordKey key, long id)
    {
      this.key = key;
      this.id = id;
      this.timestamp = 0L;
    }
  }
  
  public Stream(MetaInfo.Stream streamInfo, BaseServer srv, Flow flow)
    throws Exception
  {
    super(srv, streamInfo);
    this.streamInfo = streamInfo;
    setFlow(flow);
    if (flow != null) {
      this.isRecoveryEnabled = flow.recoveryIsEnabled();
    }
    if (srv == null)
    {
      if (streamInfo.pset != null) {
        this.output = new KafkaChannel(this);
      } else {
        this.output = new ZMQChannel(this);
      }
    }
    else if (streamInfo.pset != null) {
      this.output = new KafkaChannel(this);
    } else {
      this.output = srv.getDistributedChannel(this);
    }
    this.hasDelayBuffer = (streamInfo.gracePeriodInterval != null);
    if (this.hasDelayBuffer)
    {
      this.interval = TimeUnit.MICROSECONDS.toNanos(streamInfo.gracePeriodInterval.value);
      
      this.keyFactory = KeyFactory.createKeyFactory(streamInfo, Collections.singletonList(streamInfo.gracePeriodField), streamInfo.dataType, srv);
      
      this.index = new ConcurrentSkipListMap(comparator);
      this.nextGen = new AtomicLong(1L);
    }
  }
  
  private void addToAllTungstenShowStreams(String queueName)
  {
    Integer count = (Integer)allTungstenShowStreams.get(queueName);
    count = Integer.valueOf(count == null ? 1 : count.intValue() + 1);
    allTungstenShowStreams.put(queueName, count);
  }
  
  private void removeFromAllTungstenShowStreams(String queueName)
  {
    Integer count = (Integer)allTungstenShowStreams.get(queueName);
    count = Integer.valueOf(count == null ? 0 : count.intValue() - 1);
    if (count.intValue() > 0) {
      allTungstenShowStreams.put(queueName, count);
    } else {
      allTungstenShowStreams.remove(queueName);
    }
  }
  
  public void initQueue(MetaInfo.ShowStream showStream)
  {
    String queueName = "consoleQueue" + showStream.session_id.toString();
    synchronized (this.activeShows)
    {
      if (showStream.show)
      {
        ActiveShowStream ass = (ActiveShowStream)this.activeShows.get(queueName);
        if (ass == null)
        {
          ass = new ActiveShowStream(null);
          ass.consoleQueue = WAQueue.getQueue(queueName);
          ass.consoleQueue.isActive = true;
          this.activeShows.put(queueName, ass);
          if (showStream.isTungsten) {
            addToAllTungstenShowStreams(queueName);
          }
        }
        if (showStream.line_count > 0) {
          ass.limit_queue = new AtomicInteger(showStream.line_count);
        }
      }
      else if (this.activeShows.containsKey(queueName))
      {
        this.activeShows.remove(queueName);
        if (showStream.isTungsten) {
          removeFromAllTungstenShowStreams(queueName);
        }
      }
    }
  }
  
  public void close()
    throws IOException
  {
    resetProcessThread();
    if (this.output != null)
    {
      this.output.close();
      this.output = null;
    }
    if (this.task != null)
    {
      this.task.cancel(true);
      this.task = null;
    }
    if (logger.isInfoEnabled()) {
      logger.info("Closing stream: " + getMetaFullName());
    }
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
      if (this.activeShows.size() > 0) {
        synchronized (this.activeShows)
        {
          List<String> removeList = null;
          for (Map.Entry<String, ActiveShowStream> showEntry : this.activeShows.entrySet())
          {
            ActiveShowStream show = (ActiveShowStream)showEntry.getValue();
            if (((show.limit_queue == null) || ((show.limit_queue != null) && (show.limit_queue.get() != 0))) && (show.consoleQueue.isActive))
            {
              for (WAEvent E : event.batch()) {
                show.consoleQueue.put(new ShowStreamEvent(this.streamInfo, E));
              }
              show.limit_queue.decrementAndGet();
            }
            else
            {
              if (removeList == null) {
                removeList = new ArrayList();
              }
              removeList.add(showEntry.getKey());
            }
          }
          if (removeList != null) {
            for (String queueName : removeList)
            {
              this.activeShows.remove(queueName);
              removeFromAllTungstenShowStreams(queueName);
            }
          }
        }
      }
      if (this.hasDelayBuffer) {
        addToDelayBuffer(event);
      } else if (this.output != null) {
        this.output.publish(event);
      }
      synchronized (this)
      {
        this.inputTotal += 1L;
      }
    }
    catch (Exception ex)
    {
      logger.error("exception receiving events by stream:" + this.streamInfo.name);
      notifyAppMgr(EntityType.STREAM, getMetaName(), getMetaID(), ex, "stream receive", new Object[] { event });
      throw new Exception(ex);
    }
  }
  
  public Channel getChannel()
  {
    return this.output;
  }
  
  public void addSpecificMonitorEvents(MonitorEventsCollection monEvs)
  {
    Long it = Long.valueOf(this.inputTotal);
    if (this.output != null)
    {
      boolean isFull = this.output.isFull();
      long timeStamp = monEvs.getTimeStamp();
      if (!it.equals(this.prevInputTotal))
      {
        monEvs.add(MonitorEvent.Type.INPUT, it);
        monEvs.add(MonitorEvent.Type.LATEST_ACTIVITY, Long.valueOf(System.currentTimeMillis()));
      }
      if ((this.prevTimeStamp != null) && (timeStamp != this.prevTimeStamp.longValue()))
      {
        Long ir = Long.valueOf(Math.ceil(1000.0D * (it.longValue() - this.prevInputTotal.longValue()) / (timeStamp - this.prevTimeStamp.longValue())));
        if (!ir.equals(this.prevInputRate))
        {
          monEvs.add(MonitorEvent.Type.RATE, ir);
          monEvs.add(MonitorEvent.Type.INPUT_RATE, ir);
          this.prevInputRate = ir;
        }
      }
      if ((this.prevFull == null) || (this.prevFull.booleanValue() != isFull)) {
        monEvs.add(MonitorEvent.Type.STREAM_FULL, String.valueOf(isFull));
      }
      this.prevInputTotal = it;
      this.prevFull = Boolean.valueOf(isFull);
      if ((this.output instanceof KafkaChannel))
      {
        Map sMap = ((KafkaChannel)this.output).getStats();
        
        ((KafkaChannel)this.output).addSpecificMonitorEvents(monEvs);
      }
    }
  }
  
  public String toString()
  {
    return getMetaUri() + " - " + getMetaID() + " RUNTIME";
  }
  
  public DistributedChannel getDistributedChannel()
  {
    return this.output;
  }
  
  public void connectedFlow(Flow f)
  {
    this.connectedFlows.put(f.getMetaID(), f);
  }
  
  public void disconnectFlow(Flow f)
  {
    this.connectedFlows.remove(f.getMetaID());
  }
  
  public boolean isConnected()
  {
    return !this.connectedFlows.isEmpty();
  }
  
  private void addToDelayBuffer(ITaskEvent event)
  {
    boolean wasEmpty = this.index.isEmpty();
    long curtime = System.nanoTime();
    for (WAEvent ev : event.batch())
    {
      Object o = ev.data;
      long id = this.nextGen.getAndIncrement();
      RecordKey key = this.keyFactory.makeKey(o);
      
      Key k = new Key(key, id);
      
      Key higher = (Key)this.index.higherKey(k);
      
      k.timestamp = (higher == null ? curtime : higher.timestamp);
      
      this.index.put(k, o);
    }
    if (wasEmpty) {
      schedule(this.interval);
    }
  }
  
  private void schedule(long delay)
  {
    ScheduledExecutorService service = srv().getScheduler();
    this.task = service.schedule(this, delay, TimeUnit.NANOSECONDS);
  }
  
  public void run()
  {
    try
    {
      for (;;)
      {
        long curtime = System.nanoTime();
        
        Key k = (Key)this.index.firstKey();
        if (k.timestamp + this.interval + 50000L > curtime)
        {
          long leftToWait = k.timestamp + this.interval - curtime;
          schedule(leftToWait);
          break;
        }
        Map.Entry<Key, Object> e = this.index.pollFirstEntry();
        Key key = (Key)e.getKey();
        Object val = e.getValue();
        if ((this.boundKey == null) || (key.key.compareTo(this.boundKey) >= 0))
        {
          this.boundKey = key.key;
          this.output.publish(StreamEventFactory.createStreamEvent(val));
        }
      }
    }
    catch (NoSuchElementException e) {}catch (RejectedExecutionException e)
    {
      logger.warn("stream delay buffer terminated due to shutdown");
    }
    catch (Throwable e)
    {
      logger.error(e);
    }
  }
  
  public Position getCheckpoint()
  {
    if (this.output == null) {
      return null;
    }
    Position result = this.output.getCheckpoint();
    if (this.hasDelayBuffer) {}
    return result;
  }
  
  public MetaInfo.Stream getStreamMeta()
  {
    return this.streamInfo;
  }
  
  public synchronized void stop()
  {
    if (this.output != null) {
      this.output.stop();
    }
  }
  
  public void start()
  {
    if (this.output != null) {
      this.output.start();
    }
  }
  
  public void start(Map<UUID, Set<UUID>> servers)
  {
    if (this.output != null) {
      this.output.start(servers);
    }
  }
  
  public boolean verifyStart(Map<UUID, Set<UUID>> serverToDeployedObjects)
  {
    if (this.output != null) {
      return this.output.verifyStart(serverToDeployedObjects);
    }
    return true;
  }
  
  public void cleanChannelSubscriber(String uuid)
  {
    if (getMetaInfo().pset == null) {
      this.output.closeDistSub(uuid);
    }
  }
  
  public MetaInfo.Stream getMetaInfo()
  {
    return this.streamInfo;
  }
  
  private static final Comparator<Key> comparator = new Comparator()
  {
    public int compare(Stream.Key k1, Stream.Key k2)
    {
      int cmp = k1.key.compareTo(k2.key);
      if (cmp != 0) {
        return cmp;
      }
      if (k1.id < k2.id) {
        return -1;
      }
      if (k1.id > k2.id) {
        return 1;
      }
      return 0;
    }
  };
  
  public void setPositionAndStartEmitting(PartitionedSourcePosition position)
    throws Exception
  {
    this.output.startEmitting(position);
  }
  
  public boolean isRecoveryEnabled()
  {
    return this.isRecoveryEnabled;
  }
  
  public void setRecoveryEnabled(boolean isRecoveryEnabled)
  {
    if (Logger.getLogger("KafkaStreams").isDebugEnabled()) {
      Logger.getLogger("KafkaStreams").debug("Stream " + getMetaName() + " set recovery=" + isRecoveryEnabled);
    }
    this.isRecoveryEnabled = isRecoveryEnabled;
  }
}
