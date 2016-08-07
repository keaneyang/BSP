package com.bloom.runtime.components;

import com.bloom.classloading.WALoader;
import com.bloom.classloading.BundleDefinition.Type;
import com.bloom.runtime.BaseServer;
import com.bloom.runtime.Interval;
import com.bloom.runtime.KeyFactory;
import com.bloom.runtime.Pair;
import com.bloom.runtime.StreamEventFactory;
import com.bloom.runtime.channels.Channel;
import com.bloom.runtime.meta.MetaInfo.Sorter;
import com.bloom.runtime.meta.MetaInfo.Stream;
import com.bloom.runtime.meta.MetaInfo.Sorter.SorterRule;
import com.bloom.runtime.monitor.MonitorEventsCollection;
import com.bloom.runtime.RecordKey;
import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.ITaskEvent;
import com.bloom.runtime.containers.WAEvent;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Logger;

public class Sorter
  extends FlowComponent
  implements PubSub, Restartable, Compound, Runnable
{
  static class InOutChannel
  {
    Publisher dataSource;
    Subscriber dataSink;
    Channel output;
    KeyFactory keyFactory;
  }
  
  static class Key
  {
    final RecordKey key;
    final long id;
    long timestamp;
    final int channel_index;
    
    Key(RecordKey key, int channel_index, long id)
    {
      this.key = key;
      this.channel_index = channel_index;
      this.id = id;
      this.timestamp = 0L;
    }
  }
  
  private static final Comparator<Key> comparator = new Comparator()
  {
    public int compare(Sorter.Key k1, Sorter.Key k2)
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
  private static Logger logger = Logger.getLogger(Sorter.class);
  private final MetaInfo.Sorter sorterInfo;
  private final InOutChannel[] channels;
  private Subscriber errorSink;
  private final Channel errorOutput;
  private volatile boolean running = false;
  private final ConcurrentSkipListMap<Key, Object> index = new ConcurrentSkipListMap(comparator);
  private final AtomicLong nextGen = new AtomicLong(1L);
  private Future<?> task;
  private final long interval;
  private RecordKey boundKey;
  private final Pair<String, String>[] streamInfos;
  
  public Sorter(MetaInfo.Sorter sorterInfo, BaseServer srv)
    throws Exception
  {
    super(srv, sorterInfo);
    this.sorterInfo = sorterInfo;
    this.interval = TimeUnit.MICROSECONDS.toNanos(sorterInfo.sortTimeInterval.value);
    this.errorOutput = srv.createSimpleChannel();
    this.channels = new InOutChannel[sorterInfo.inOutRules.size()];
    Iterator<MetaInfo.Sorter.SorterRule> it = sorterInfo.inOutRules.iterator();
    
    this.streamInfos = new Pair[sorterInfo.inOutRules.size()];
    for (int i = 0; it.hasNext(); i++)
    {
      MetaInfo.Sorter.SorterRule r = (MetaInfo.Sorter.SorterRule)it.next();
      InOutChannel c = this.channels[i] = new InOutChannel();
      c.output = srv.createSimpleChannel();
      MetaInfo.Stream streamInfo = srv.getStreamInfo(r.inStream);
      String uniqueID = "_" + System.nanoTime();
      this.streamInfos[i] = Pair.make(streamInfo.nsName, streamInfo.name + uniqueID);
      c.keyFactory = KeyFactory.createKeyFactory(streamInfo.nsName, streamInfo.name + uniqueID, Collections.singletonList(r.inStreamField), r.inOutType, srv);
    }
  }
  
  public Channel getChannel()
  {
    return this.channels[0].output;
  }
  
  public void close()
    throws Exception
  {
    stop();
    for (InOutChannel c : this.channels) {
      c.output.close();
    }
    for (Pair<String, String> stream : this.streamInfos)
    {
      WALoader wal = WALoader.get();
      wal.removeBundle((String)stream.first, BundleDefinition.Type.keyFactory, (String)stream.second);
    }
  }
  
  public void addSpecificMonitorEvents(MonitorEventsCollection monEvs) {}
  
  public synchronized void receive(Object linkID, ITaskEvent event)
    throws Exception
  {
    boolean wasEmpty = this.index.isEmpty();
    int channel_index = ((Integer)linkID).intValue();
    long curtime = System.nanoTime();
    
    IBatch<WAEvent> waEventIBatch = event.batch();
    for (WAEvent ev : waEventIBatch)
    {
      Object o = ev.data;
      long id = this.nextGen.getAndIncrement();
      RecordKey key = this.channels[channel_index].keyFactory.makeKey(o);
      
      Key k = new Key(key, channel_index, id);
      
      Key higher = (Key)this.index.higherKey(k);
      
      k.timestamp = (higher == null ? curtime : higher.timestamp);
      
      this.index.put(k, o);
    }
    if (wasEmpty) {
      schedule(this.interval);
    }
  }
  
  public void connectParts(Flow flow)
    throws Exception
  {
    Iterator<MetaInfo.Sorter.SorterRule> it = this.sorterInfo.inOutRules.iterator();
    for (InOutChannel c : this.channels)
    {
      MetaInfo.Sorter.SorterRule r = (MetaInfo.Sorter.SorterRule)it.next();
      c.dataSource = flow.getPublisher(r.inStream);
      c.dataSink = flow.getSubscriber(r.outStream);
    }
    this.errorSink = flow.getSubscriber(this.sorterInfo.errorStream);
  }
  
  public void start()
    throws Exception
  {
    if (this.running) {
      return;
    }
    this.running = true;
    this.boundKey = null;
    int i = 0;
    BaseServer s = srv();
    for (InOutChannel c : this.channels)
    {
      s.subscribe(c.dataSource, new Link(this, i));
      s.subscribe(c.output, new Link(c.dataSink));
      i++;
    }
    s.subscribe(this.errorOutput, new Link(this.errorSink));
  }
  
  public void stop()
    throws Exception
  {
    if (!this.running) {
      return;
    }
    this.running = false;
    int i = 0;
    BaseServer s = srv();
    for (InOutChannel c : this.channels)
    {
      s.unsubscribe(c.dataSource, new Link(this, i));
      s.unsubscribe(c.output, new Link(c.dataSink));
      i++;
    }
    s.unsubscribe(this.errorOutput, new Link(this.errorSink));
    if (this.task != null)
    {
      this.task.cancel(true);
      this.task = null;
    }
  }
  
  public boolean isRunning()
  {
    return this.running;
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
        if (!this.running) {
          return;
        }
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
          this.channels[key.channel_index].output.publish(StreamEventFactory.createStreamEvent(val));
        }
        else
        {
          this.errorOutput.publish(StreamEventFactory.createStreamEvent(val));
        }
      }
    }
    catch (NoSuchElementException e) {}catch (RejectedExecutionException e)
    {
      logger.warn("sorter terminated due to shutdown");
    }
    catch (Throwable e)
    {
      logger.error(e);
    }
  }
}
