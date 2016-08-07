package com.bloom.runtime.window;

import com.bloom.runtime.KeyFactory;
import com.bloom.runtime.Server;
import com.bloom.runtime.channels.Channel;
import com.bloom.runtime.channels.Channel.NewSubscriberAddedCallback;
import com.bloom.runtime.components.Flow;
import com.bloom.runtime.components.FlowComponent;
import com.bloom.runtime.components.IWindow;
import com.bloom.runtime.components.Link;
import com.bloom.runtime.components.Publisher;
import com.bloom.runtime.components.Subscriber;
import com.bloom.runtime.containers.Batch;
import com.bloom.runtime.containers.Range;
import com.bloom.runtime.containers.TaskEvent;
import com.bloom.runtime.meta.IntervalPolicy;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.IntervalPolicy.CountBasedPolicy;
import com.bloom.runtime.meta.IntervalPolicy.TimeBasedPolicy;
import com.bloom.runtime.meta.MetaInfo.Window;
import com.bloom.runtime.monitor.MonitorEvent;
import com.bloom.runtime.monitor.MonitorEventsCollection;
import com.bloom.runtime.RecordKey;
import com.bloom.runtime.containers.ITaskEvent;
import com.bloom.runtime.containers.WAEvent;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

public class ScalaWindow
  extends IWindow
  implements Runnable
{
  private static Logger logger = Logger.getLogger(Window.class);
  private final MetaInfo.Window windowInfo;
  private final Channel output;
  private volatile boolean running;
  private final ServerWrapper srv;
  private Publisher dataSource;
  private final WTimerPolicy timerPolicy;
  private final WBufferFactory windowBufferFactory;
  private final WBufferPolicy bufferPolicy;
  final int count;
  final long interval;
  final CmpAttrs attrComparator;
  
  ScalaWindow(MetaInfo.Window windowInfo, ServerWrapper srv, WTimerPolicy tp, WBufferFactory wf, WBufferPolicy bp, int count, long interval, CmpAttrs comparator)
    throws Exception
  {
    super(srv.getServer(), windowInfo);
    this.windowInfo = windowInfo;
    this.output = srv.createChannel(this);
    this.output.addCallback(this);
    this.srv = srv;
    this.timerPolicy = tp;
    this.windowBufferFactory = wf;
    this.bufferPolicy = bp;
    this.count = count;
    this.interval = interval;
    this.attrComparator = comparator;
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
    this.srv.destroyKeyFactory(this.windowInfo);
  }
  
  public void start()
    throws Exception
  {
    if (this.running) {
      return;
    }
    this.running = true;
    
    this.bufferPolicy.initBuffer(this);
    
    this.timerPolicy.startTimer(this);
    
    this.srv.subscribe(this.dataSource, this);
  }
  
  public void stop()
    throws Exception
  {
    if (!this.running) {
      return;
    }
    this.running = false;
    
    this.srv.unsubscribe(this.dataSource, this);
    
    this.timerPolicy.stopTimer(this);
  }
  
  public boolean isRunning()
  {
    return this.running;
  }
  
  public void connectParts(Flow flow)
    throws Exception
  {
    this.dataSource = flow.getPublisher(this.windowInfo.stream);
  }
  
  public synchronized void receive(Object linkID, ITaskEvent event)
    throws Exception
  {
    this.bufferPolicy.updateBuffer(this, (Batch)event.batch(), System.nanoTime());
  }
  
  public synchronized void run()
  {
    try
    {
      this.timerPolicy.onTimer(this);
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }
  
  public void notifyMe(Link link)
  {
    Range range = this.bufferPolicy.createSnapshot();
    TaskEvent ws = TaskEvent.createWindowStateEvent(range);
    try
    {
      link.subscriber.receive(Integer.valueOf(link.linkID), ws);
    }
    catch (Exception e)
    {
      logger.error(e);
    }
  }
  
  final void publish(Batch added, List<WAEvent> removed, Range snapshot)
    throws Exception
  {
    this.output.publish(TaskEvent.createWindowEvent(added, Batch.asBatch(removed), snapshot));
  }
  
  final void updateBuffer(WBuffer w, Batch batch, long now)
    throws Exception
  {
    w.update(batch, now);
    this.timerPolicy.updateWakeupQueue(this, w, now);
  }
  
  final WBuffer createBuffer(RecordKey key)
  {
    WBuffer b = this.windowBufferFactory.create();
    b.setOwner(this, key);
    return b;
  }
  
  final ScheduledExecutorService getScheduler()
  {
    return this.srv.getScheduler();
  }
  
  final void jumpingWindowOnTimerCallback()
    throws Exception
  {
    this.bufferPolicy.onJumpingTimer();
  }
  
  public static ServerWrapper makeServerWrapper(Server srv)
  {
    new ServerWrapper()
    {
      public Channel createChannel(FlowComponent owner)
      {
        return srv.createChannel(owner);
      }
      
      public void destroyKeyFactory(MetaInfo.Window windowInfo)
        throws Exception
      {
        KeyFactory.removeKeyFactory(windowInfo, srv);
      }
      
      public void subscribe(Publisher pub, Subscriber sub)
        throws Exception
      {
        srv.subscribe(pub, sub);
      }
      
      public void unsubscribe(Publisher pub, Subscriber sub)
        throws Exception
      {
        srv.unsubscribe(pub, sub);
      }
      
      public ScheduledExecutorService getScheduler()
      {
        return srv.getScheduler();
      }
      
      public Server getServer()
      {
        return srv;
      }
    };
  }
  
  public static ServerWrapper makeTestServerWrapper(Subscriber sub, final ScheduledExecutorService executor)
  {
    Channel output = new Channel()
    {
      public void close()
        throws IOException
      {}
      
      public Collection<MonitorEvent> getMonitorEvents(long ts)
      {
        return null;
      }
      
      public void publish(ITaskEvent event)
        throws Exception
      {
        sub.receive(null, event);
      }
      
      public void addSubscriber(Link link) {}
      
      public void removeSubscriber(Link link) {}
      
      public void addCallback(Channel.NewSubscriberAddedCallback callback) {}
      
      public int getSubscribersCount()
      {
        return 0;
      }
    };
    new ServerWrapper()
    {
      public Channel createChannel(FlowComponent owner)
      {
        return output;
      }
      
      public void destroyKeyFactory(MetaInfo.Window windowInfo) {}
      
      public void subscribe(Publisher pub, Subscriber sub) {}
      
      public void unsubscribe(Publisher pub, Subscriber sub) {}
      
      public ScheduledExecutorService getScheduler()
      {
        return executor;
      }
      
      public Server getServer()
      {
        return null;
      }
    };
  }
  
  public static IWindow createWindow(MetaInfo.Window w, Server srv)
    throws Exception
  {
    IntervalPolicy l = w.windowLen;
    boolean jumping = w.jumping;
    boolean attrBased = l.isAttrBased();
    boolean countBased = l.isCountBased();
    boolean timeBased = l.isTimeBased();
    int count = countBased ? l.getCountPolicy().getCountInterval() : 0;
    long interval = timeBased ? TimeUnit.MICROSECONDS.toNanos(l.getTimePolicy().getTimeInterval()) : 0L;
    
    CmpAttrs attrComparator = attrBased ? AttrExtractor.createAttrComparator(w, srv) : null;
    
    KeyFactory keyFactory = w.partitioningFields.isEmpty() ? null : KeyFactory.createKeyFactory(w, srv);
    
    ServerWrapper serverWrapper = makeServerWrapper(srv);
    
    return createWindow(w, jumping, attrBased, countBased, timeBased, count, interval, attrComparator, keyFactory, serverWrapper);
  }
  
  public static IWindow createWindow(MetaInfo.Window w, boolean jumping, boolean attrBased, boolean countBased, boolean timeBased, int count, long interval_nano, CmpAttrs attrComparator, KeyFactory keyFactory, ServerWrapper serverWrapper)
    throws Exception
  {
    WBufferFactory bufferFactory;
    if (attrBased)
    {
      if (jumping) {
        bufferFactory = new WBufferFactory()
        {
          public WBuffer create()
          {
            return new WBufferJumpingAttr();
          }
        };
      } else {
        bufferFactory = new WBufferFactory()
        {
          public WBuffer create()
          {
            return new WBufferSlidingAttr();
          }
        };
      }
    }
    else
    {
      if ((countBased) && (!timeBased))
      {
        if (jumping) {
          bufferFactory = new WBufferFactory()
          {
            public WBuffer create()
            {
              return new WBufferJumpingCount();
            }
          };
        } else {
          bufferFactory = new WBufferFactory()
          {
            public WBuffer create()
            {
              return new WBufferSlidingCount();
            }
          };
        }
      }
      else
      {
        if ((!countBased) && (timeBased))
        {
          if (jumping) {
            bufferFactory = new WBufferFactory()
            {
              public WBuffer create()
              {
                return new WBufferJumpingTime();
              }
            };
          } else {
            bufferFactory = new WBufferFactory()
            {
              public WBuffer create()
              {
                return new WBufferSlidingTime();
              }
            };
          }
        }
        else
        {
          if ((countBased) && (timeBased))
          {
            if (jumping) {
              bufferFactory = new WBufferFactory()
              {
                public WBuffer create()
                {
                  return new WBufferJumpingTimeCount();
                }
              };
            } else {
              bufferFactory = new WBufferFactory()
              {
                public WBuffer create()
                {
                  return new WBufferSlidingTimeCount();
                }
              };
            }
          }
          else
          {
            return null;
          }
        }
      }
    }
    WTimerPolicy timerPolicy;
    if (timeBased)
    {
      
      if (jumping) {
        timerPolicy = new JumpingTimerPolicy();
      } else {
        timerPolicy = new SlidingTimerPolicy();
      }
    }
    else
    {
      timerPolicy = new NoTimerPolicy();
    }
    WBufferPolicy bufferPolicy;
    if (keyFactory != null)
    {
      if ((!jumping) && (countBased) && (!timeBased) && (count == 1)) {
        bufferPolicy = new PartitionedBufferPolicy(keyFactory);
      } else {
        bufferPolicy = new PartitionedBufferPolicy(keyFactory);
      }
    }
    else
    {
      bufferPolicy = new SimpleBufferPolicy();
    }
    return new ScalaWindow(w, serverWrapper, timerPolicy, bufferFactory, bufferPolicy, count, interval_nano, attrComparator);
  }
  
  public void addSpecificMonitorEvents(MonitorEventsCollection events) {}
}
