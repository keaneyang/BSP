package com.bloom.runtime.channels;

import com.bloom.runtime.BaseServer;
import com.bloom.runtime.components.FlowComponent;
import com.bloom.runtime.monitor.MonitorEvent;
import com.bloom.runtime.monitor.MonitorEventsCollection;
import com.bloom.runtime.monitor.MonitorEvent.Type;
import com.bloom.runtime.containers.ITaskEvent;

import java.io.IOException;
import java.nio.channels.ClosedSelectorException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import org.zeromq.ZMQException;

import zmq.ZError;

public class BroadcastAsyncChannel
  extends BroadcastChannel
  implements Runnable
{
  private static Logger logger = Logger.getLogger(BroadcastAsyncChannel.class);
  private final BlockingQueue<ITaskEvent> eventQueue = new LinkedBlockingQueue(100);
  private volatile boolean running;
  private Future<?> task;
  
  public BroadcastAsyncChannel(FlowComponent owner)
  {
    super(owner);
    this.running = true;
    ExecutorService pool = srv().getThreadPool();
    this.task = pool.submit(this);
  }
  
  public void publish(ITaskEvent event)
    throws Exception
  {
    if (this.running)
    {
      setProcessThread();
      this.eventQueue.put(event);
      this.received += 1L;
    }
  }
  
  public void run()
  {
    while (this.running) {
      try
      {
        ITaskEvent event = (ITaskEvent)this.eventQueue.poll(100L, TimeUnit.MILLISECONDS);
        if (event != null) {
          doPublish(event);
        }
      }
      catch (RejectedExecutionException e)
      {
        logger.info(getOwnerMetaName() + " channel interrupted");
        break;
      }
      catch (InterruptedException e)
      {
        logger.info(getOwnerMetaName() + " channel interrupted");
        break;
      }
      catch (ZError.IOException|ClosedSelectorException|ZMQException e)
      {
        logger.debug("Got ZMQException " + e.getMessage());
      }
      catch (com.bloom.exception.RuntimeInterruptedException e)
      {
        logger.warn("Got RuntimeInterruptedException " + e.getMessage());
        break;
      }
      catch (com.hazelcast.core.RuntimeInterruptedException e)
      {
        logger.warn("Got RuntimeInterruptedException " + e.getMessage());
        break;
      }
      catch (Exception e)
      {
        logger.error("Problem running async broadcast channel " + getOwnerMetaName() + ": " + e.getMessage());
      }
    }
  }
  
  public void close()
    throws IOException
  {
    resetProcessThread();
    this.running = false;
    this.task.cancel(true);
    this.eventQueue.clear();
    super.close();
  }
  
  Long prevReceived = null;
  Long prevProcessed = null;
  
  public void addSpecificMonitorEvents(MonitorEventsCollection monEvs)
  {
    long r = this.received;
    long p = this.processed;
    
    long timeStamp = monEvs.getTimeStamp();
    if (this.prevReceived.longValue() != r)
    {
      monEvs.add(MonitorEvent.Type.RECEIVED, Long.valueOf(r));
      if ((this.prevReceived != null) && (this.prevProcessed != null) && (this.prevTimeStamp != null))
      {
        monEvs.add(MonitorEvent.Type.RATE, Long.valueOf(1000L * (r - this.prevReceived.longValue()) / (timeStamp - this.prevTimeStamp.longValue())));
        monEvs.add(MonitorEvent.Type.RECEIVED_RATE, Long.valueOf(1000L * (r - this.prevReceived.longValue()) / (timeStamp - this.prevTimeStamp.longValue())));
      }
    }
    if (this.prevProcessed.longValue() != p)
    {
      monEvs.add(MonitorEvent.Type.PROCESSED, Long.valueOf(p));
      if ((this.prevReceived != null) && (this.prevProcessed != null) && (this.prevTimeStamp != null)) {
        monEvs.add(MonitorEvent.Type.PROCESSED_RATE, Long.valueOf((p - this.prevProcessed.longValue()) / (timeStamp - this.prevTimeStamp.longValue())));
      }
    }
    this.prevReceived = Long.valueOf(r);
    this.prevProcessed = Long.valueOf(p);
  }
}
