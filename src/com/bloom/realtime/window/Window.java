package com.bloom.runtime.window;

import com.bloom.runtime.BaseServer;
import com.bloom.runtime.KeyFactory;
import com.bloom.runtime.channels.Channel;
import com.bloom.runtime.components.Flow;
import com.bloom.runtime.components.IWindow;
import com.bloom.runtime.components.Link;
import com.bloom.runtime.components.Stream;
import com.bloom.runtime.components.Subscriber;
import com.bloom.runtime.containers.Batch;
import com.bloom.runtime.containers.Range;
import com.bloom.runtime.containers.TaskEvent;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Window;
import com.bloom.runtime.monitor.MonitorEventsCollection;
import com.bloom.runtime.utils.NetLogger;
import com.bloom.recovery.Position;
import com.bloom.runtime.RecordKey;
import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.ITaskEvent;
import com.bloom.runtime.containers.WAEvent;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

public class Window
  extends IWindow
  implements BufWindowSub
{
  private static Logger logger = Logger.getLogger(Window.class);
  private final MetaInfo.Window windowInfo;
  private final Channel output;
  private BufWindowFactory wndDesc;
  private Stream dataSource;
  private volatile boolean running = false;
  private volatile boolean closed = false;
  private BufferManager bufMgr;
  private Future<?> statsTask;
  private volatile PrintStream tracer;
  
  public Window(MetaInfo.Window windowInfo, BaseServer srv)
    throws Exception
  {
    super(srv, windowInfo);
    this.windowInfo = windowInfo;
    this.output = srv.createChannel(this);
    this.output.addCallback(this);
  }
  
  public void connectParts(Flow flow)
    throws Exception
  {
    this.dataSource = ((Stream)flow.getPublisher(this.windowInfo.stream));
    this.wndDesc = BufWindowFactory.create(this.windowInfo, srv(), this);
    this.bufMgr = BufferManager.create(this, this.wndDesc);
  }
  
  public Channel getChannel()
  {
    return this.output;
  }
  
  public void close()
    throws Exception
  {
    if (this.closed) {
      return;
    }
    this.closed = true;
    stop();
    if (this.dataSource != null)
    {
      if ((this.wndDesc != null) && (this.wndDesc.getPolicy().getComparator() != null)) {
        AttrExtractor.removeAttrExtractor(this.windowInfo);
      }
      this.bufMgr.shutdown();
    }
    this.output.close();
    KeyFactory.removeKeyFactory(this.windowInfo, srv());
  }
  
  public void addSpecificMonitorEvents(MonitorEventsCollection monEvs)
  {
    if (this.bufMgr != null) {
      this.bufMgr.addSpecificMonitorEventsForWindow(this, monEvs);
    }
  }
  
  public void notifyMe(Link link)
  {
    Range range = this.bufMgr.makeSnapshot();
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
  
  public void receive(Object linkID, ITaskEvent event)
    throws Exception
  {
    this.bufMgr.receive(linkID, event);
  }
  
  public void start()
    throws Exception
  {
    if (this.running) {
      return;
    }
    srv().subscribe(this.dataSource, this);
    this.running = true;
    if (this.windowInfo.options != null)
    {
      int port = ((Number)this.windowInfo.options).intValue();
      this.tracer = (port == 0 ? null : NetLogger.create(port));
      this.statsTask = srv().scheduleStatsReporting(new Runnable()
      {
        public void run()
        {
          Window.this.dumpStats();
        }
      }, 2, false);
    }
  }
  
  public void stop()
    throws Exception
  {
    if (!this.running) {
      return;
    }
    srv().unsubscribe(this.dataSource, this);
    this.bufMgr.flushAll();
    this.running = false;
    if (this.statsTask != null)
    {
      this.statsTask.cancel(true);
      if (this.tracer != null)
      {
        this.tracer.close();
        this.tracer = null;
      }
    }
  }
  
  public boolean isRunning()
  {
    return this.running;
  }
  
  public Position getCheckpoint()
  {
    Position result = this.bufMgr.getCheckpoint(this);
    if (logger.isTraceEnabled()) {
      logger.trace("Returning window " + getMetaName() + " current position: " + result);
    }
    return result;
  }
  
  public void setWaitPosition(Position position)
  {
    if (logger.isDebugEnabled()) {
      logger.debug("Setting window " + getMetaName() + " wait position to " + position);
    }
    this.bufMgr.setWaitPosition(position);
  }
  
  public BufWindowFactory getWindowDesc()
  {
    return this.wndDesc;
  }
  
  public MetaInfo.Window getWindowMeta()
  {
    return this.windowInfo;
  }
  
  public void dumpStats()
  {
    PrintStream out = this.tracer == null ? NetLogger.out() : this.tracer;
    try
    {
      BufferManager.Stats s = this.bufMgr.getStats();
      
      out.println(DateTime.now().toLocalTime() + " ------- window:" + getMetaName());
      out.println(s);
    }
    catch (Throwable e)
    {
      e.printStackTrace(out);
      throw e;
    }
  }
  
  public void receive(RecordKey partKey, Collection<WAEvent> snapshot, IBatch added, IBatch removed)
  {
    String distId = partKey != null ? partKey.toPartitionKey() : null;
    
    List<WAEvent> newAdded = new ArrayList(added.size());
    for (Object o : added)
    {
      WAEvent waevent = (WAEvent)o;
      if (waevent.position == null) {
        newAdded.add(waevent);
      } else {
        try
        {
          WAEvent newInstance = (WAEvent)waevent.getClass().newInstance();
          newInstance.initValues(waevent);
          newInstance.position = newInstance.position.createAugmentedPosition(this.windowInfo.uuid, distId);
          newAdded.add(newInstance);
        }
        catch (InstantiationException e)
        {
          logger.error(e);
        }
        catch (IllegalAccessException e)
        {
          logger.error(e);
        }
      }
    }
    List<WAEvent> newRemoved = new ArrayList(removed.size());
    for (Object o : removed)
    {
      WAEvent waevent = (WAEvent)o;
      if (waevent.position == null) {
        newRemoved.add(waevent);
      } else {
        try
        {
          WAEvent newInstance = (WAEvent)waevent.getClass().newInstance();
          newInstance.initValues(waevent);
          newInstance.position = newInstance.position.createAugmentedPosition(this.windowInfo.uuid, distId);
          newRemoved.add(newInstance);
        }
        catch (InstantiationException e)
        {
          logger.error(e);
        }
        catch (IllegalAccessException e)
        {
          logger.error(e);
        }
      }
    }
    TaskEvent event = TaskEvent.createWindowEvent(Batch.asBatch(newAdded), Batch.asBatch(newRemoved), Range.createRange(partKey, snapshot));
    try
    {
      this.output.publish(event);
    }
    catch (InterruptedException e)
    {
      logger.debug("during publishing window event:" + e);
    }
    catch (Exception e)
    {
      throw new RuntimeException(e);
    }
  }
}
