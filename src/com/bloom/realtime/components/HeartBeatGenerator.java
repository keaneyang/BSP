package com.bloom.runtime.components;

import com.bloom.runtime.BaseServer;
import com.bloom.runtime.channels.Channel;
import com.bloom.runtime.containers.TaskEvent;
import com.bloom.runtime.meta.MetaInfo.StreamGenerator;
import com.bloom.runtime.monitor.MonitorEventsCollection;
import com.bloom.uuid.UUID;
import com.bloom.event.SimpleEvent;
import com.bloom.runtime.containers.WAEvent;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

public class HeartBeatGenerator
  extends IStreamGenerator
{
  private static Logger logger = Logger.getLogger(HeartBeatGenerator.class);
  
  public static class HeartBeatEvent
    extends SimpleEvent
    implements Serializable
  {
    private static final long serialVersionUID = 7990852358854948290L;
    public static UUID uuid = new UUID("1288474f-c991-455d-8de3-c2e4d08ed1c0");
    public long value;
    
    public HeartBeatEvent(long timestamp)
    {
      super();
    }
    
    public String toString()
    {
      return "heartbeat(" + this.timeStamp + "," + this.value + ")";
    }
  }
  
  private volatile boolean running = false;
  private volatile ScheduledFuture<?> task;
  private Channel output;
  private long interval;
  private long heartbeats = 0L;
  private int batchSize = 1;
  private int limit = Integer.MAX_VALUE;
  private long nextval = 0L;
  private long maxnextval = 0L;
  
  public HeartBeatGenerator(MetaInfo.StreamGenerator info, BaseServer srv)
  {
    super(srv, info);
    this.output = srv.createChannel(this);
    this.interval = ((Number)info.args[0]).longValue();
    if (info.args.length > 1)
    {
      this.limit = ((Number)info.args[1]).intValue();
      if (info.args.length > 2)
      {
        this.batchSize = ((Number)info.args[2]).intValue();
        if (info.args.length > 3) {
          this.maxnextval = ((Number)info.args[3]).longValue();
        }
      }
    }
  }
  
  void publish(long timestamp)
    throws Exception
  {
    List<WAEvent> batch = new ArrayList(this.batchSize);
    for (int i = 0; i < this.batchSize; i++)
    {
      HeartBeatEvent ev = new HeartBeatEvent(timestamp);
      if (this.nextval == this.maxnextval) {
        this.nextval = 0L;
      }
      ev.value = (this.nextval++);
      ev.setPayload(new Object[] { Long.valueOf(ev.value) });
      batch.add(new WAEvent(ev));
    }
    TaskEvent ev = TaskEvent.createStreamEvent(batch);
    this.output.publish(ev);
    this.heartbeats += 1L;
  }
  
  public void run()
  {
    try
    {
      if (!this.running) {
        return;
      }
      if (this.heartbeats == this.limit) {
        return;
      }
      long lastTimestamp = System.nanoTime() / 1000L;
      publish(lastTimestamp);
      for (;;)
      {
        if (!this.running) {
          return;
        }
        if (this.heartbeats == this.limit) {
          return;
        }
        long now = System.nanoTime() / 1000L;
        if (lastTimestamp + this.interval > now) {
          break;
        }
        publish(lastTimestamp);
        lastTimestamp = now;
      }
      schedule();
    }
    catch (RejectedExecutionException e)
    {
      logger.warn("generator terminated due to shutdown");
    }
    catch (InterruptedException e) {}catch (Throwable e)
    {
      logger.error(e);
    }
  }
  
  public void close()
    throws Exception
  {
    stop();
    this.output.close();
  }
  
  private void schedule()
  {
    ScheduledExecutorService service = srv().getScheduler();
    this.task = service.schedule(this, this.interval, TimeUnit.MICROSECONDS);
  }
  
  public Channel getChannel()
  {
    return this.output;
  }
  
  public void addSpecificMonitorEvents(MonitorEventsCollection monEvs) {}
  
  public void start()
    throws Exception
  {
    if (this.running) {
      return;
    }
    this.running = true;
    schedule();
  }
  
  public void stop()
    throws Exception
  {
    if (!this.running) {
      return;
    }
    this.running = false;
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
}
