package com.bloom.runtime.window;

import java.io.PrintStream;
import java.lang.ref.ReferenceQueue;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import org.apache.log4j.Logger;

import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.runtime.BaseServer;
import com.bloom.runtime.KeyFactory;
import com.bloom.runtime.Server;
import com.bloom.runtime.ServerServices;
import com.bloom.runtime.containers.Range;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Window;
import com.bloom.runtime.monitor.MonitorEventsCollection;
import com.bloom.uuid.UUID;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.bloom.recovery.Position;
import com.bloom.recovery.PositionResponse;
import com.bloom.runtime.RecordKey;
import com.bloom.runtime.containers.ITaskEvent;

public abstract class BufferManager
  implements MessageListener<PositionResponse>
{
  public static boolean removeEmptyPartitions = true;
  protected static final Logger logger = Logger.getLogger(BufferManager.class);
  static SnapshotRefIndexFactory snIndexFac = FibonacciHeapSnapshotRefIndex.factory;
  private volatile int inputCount = 0;
  private final UUID windowid;
  private final ReferenceQueue<Snapshot> rqueue = new ReferenceQueue();
  private final Future<?> remover;
  private volatile boolean finished = false;
  private final BufWindowFactory windowDesc;
  protected Map<UUID, Position> windowCheckpoints = new HashMap();
  private String positionRequestMessageListenerRegId = null;
  Map<BufWindowFactory, Position> bufferCheckpoints = new HashMap();
  
  BufferManager(UUID windowid, BufWindowFactory windowDesc, ServerServices srv)
  {
    this.windowid = windowid;
    this.windowDesc = windowDesc;
    this.remover = srv.getThreadPool().submit(new Runnable()
    {
      public void run()
      {
        try
        {
          for (;;)
          {
            if (!BufferManager.this.finished) {
              try
              {
                SnapshotRef ref = (SnapshotRef)BufferManager.this.rqueue.remove();
                BufWindow buffer = ref.buffer;
                buffer.removeRefAndSetOldestSnapshot(ref);
              }
              catch (InterruptedException e) {}
            }
          }
        }
        catch (Throwable e)
        {
          e.printStackTrace();
          throw e;
        }
      }
    });
    ITopic<PositionResponse> topic = HazelcastSingleton.get().getTopic("#SharedCheckpoint_" + windowid);
    this.positionRequestMessageListenerRegId = topic.addMessageListener(this);
  }
  
  public UUID getWindowID()
  {
    return this.windowid;
  }
  
  public BufWindowFactory getFactory()
  {
    return this.windowDesc;
  }
  
  public abstract Stats getStats();
  
  public abstract void flushAll();
  
  public abstract Position getCheckpoint(Window paramWindow);
  
  public abstract Range makeSnapshot();
  
  public abstract void dumpActiveSnapshots(PrintStream paramPrintStream);
  
  public abstract void addSpecificMonitorEventsForWindow(Window paramWindow, MonitorEventsCollection paramMonitorEventsCollection);
  
  public abstract void receiveImpl(Object paramObject, ITaskEvent paramITaskEvent)
    throws Exception;
  
  abstract void closeBuffers();
  
  abstract void dumpBuffer(PrintStream paramPrintStream);
  
  abstract void addEmptyBuffer(BufWindow paramBufWindow);
  
  abstract ScheduledExecutorService getScheduler();
  
  public static BufferManager create(Window window, BufWindowFactory windowDesc)
  {
    MetaInfo.Window wi = window.getWindowMeta();
    BaseServer srv = window.srv();
    UUID windowid = wi.getUuid();
    try
    {
      if (wi.partitioningFields.isEmpty()) {
        return new NoPartBufferManager(windowid, windowDesc, srv);
      }
      KeyFactory kf = KeyFactory.createKeyFactory(wi, srv);
      return new PartitionedBufferManager(windowid, windowDesc, kf, srv);
    }
    catch (Throwable e)
    {
      throw new RuntimeException(e);
    }
  }
  
  public static BufferManager createTestBufferManager(BufWindowFactory windowDesc, ScheduledExecutorService scheduler, final ExecutorService es)
  {
    new NoPartBufferManager(null, windowDesc, new ServerServices()
    {
      public ScheduledExecutorService getScheduler()
      {
        return this.val$scheduler;
      }
      
      public ExecutorService getThreadPool()
      {
        return es;
      }
      
      public ScheduledFuture<?> scheduleStatsReporting(Runnable task, int period, boolean opt)
      {
        return null;
      }
    });
  }
  
  public static BufferManager createPartitionedTestBufferManager(BufWindowFactory windowDesc, ScheduledExecutorService scheduler, final ExecutorService es, KeyFactory kf)
  {
    new PartitionedBufferManager(null, windowDesc, kf, new ServerServices()
    {
      public ScheduledExecutorService getScheduler()
      {
        return this.val$scheduler;
      }
      
      public ExecutorService getThreadPool()
      {
        return es;
      }
      
      public ScheduledFuture<?> scheduleStatsReporting(Runnable task, int period, boolean opt)
      {
        return null;
      }
    });
  }
  
  public void receive(Object linkID, ITaskEvent event)
    throws Exception
  {
    this.inputCount += event.batch().size();
    
    receiveImpl(linkID, event);
  }
  
  public int getInputCount()
  {
    return this.inputCount;
  }
  
  public void shutdown()
  {
    this.finished = true;
    if (this.remover != null) {
      this.remover.cancel(true);
    }
    this.windowDesc.removeAllSubsribers();
    closeBuffers();
    
    ITopic<PositionResponse> topic = HazelcastSingleton.get().getTopic("#SharedCheckpoint_" + this.windowid);
    topic.removeMessageListener(this.positionRequestMessageListenerRegId);
  }
  
  ReferenceQueue<Snapshot> getRefQueue()
  {
    return this.rqueue;
  }
  
  public String toString()
  {
    return "BufferManager(" + this.windowDesc + ")";
  }
  
  protected BufWindow createBuffer(RecordKey partKey)
  {
    BufWindow b = this.windowDesc.createWindow(this);
    b.setPartKey(partKey);
    return b;
  }
  
  public final void setWaitPosition(Position position)
  {
    flushAll();
    this.windowCheckpoints.put(this.windowid, position);
  }
  
  public void onMessage(Message<PositionResponse> message)
  {
    PositionResponse pr = (PositionResponse)message.getMessageObject();
    if (!Server.server.getServerID().equals(pr.fromServer))
    {
      Position dataPositionAugmented = pr.position;
      if (logger.isDebugEnabled()) {
        logger.debug("BufferManagerImpl received shared-checkpoint notify of removal for " + dataPositionAugmented);
      }
      Position bufferCheckpoint = (Position)this.windowCheckpoints.get(pr.fromID);
      if (bufferCheckpoint != null) {
        bufferCheckpoint.removePathsWhichStartWith(dataPositionAugmented.values());
      }
    }
  }
  
  public static abstract class Stats
  {
    public abstract String toString();
  }
}
