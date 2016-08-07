package com.bloom.runtime.window;

import com.bloom.runtime.CustomThreadFactory;
import com.bloom.runtime.KeyFactory;
import com.bloom.runtime.Server;
import com.bloom.runtime.ServerServices;
import com.bloom.runtime.containers.Batch;
import com.bloom.runtime.containers.Range;
import com.bloom.runtime.monitor.MonitorEvent;
import com.bloom.runtime.monitor.MonitorEventsCollection;
import com.bloom.runtime.monitor.MonitorEvent.Type;
import com.bloom.utility.Utility;
import com.bloom.uuid.UUID;
import com.bloom.recovery.Position;
import com.bloom.runtime.RecordKey;
import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.ITaskEvent;
import com.bloom.runtime.containers.WAEvent;

import java.io.PrintStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.log4j.Logger;

class PartitionedBufferManager
  extends BufferManager
{
  private final Queue<BufWindow> emptyBuffers = new ConcurrentLinkedQueue();
  private final Future<?> removeEmptyBufsTask;
  private final Lock bufLock = new ReentrantLock();
  private final ConcurrentHashMap<RecordKey, BufWindow> buffers = new ConcurrentHashMap();
  private final KeyFactory keyFactory;
  private final ScheduledThreadPoolExecutor scheduler;
  private volatile int createdPartitions = 0;
  private volatile int removedPartitions = 0;
  
  PartitionedBufferManager(UUID windowid, BufWindowFactory windowDesc, KeyFactory keyFactory, ServerServices srv)
  {
    super(windowid, windowDesc, srv);
    this.keyFactory = keyFactory;
    String wname = windowid.getUUIDString();
    this.scheduler = new ScheduledThreadPoolExecutor(1, new CustomThreadFactory("Window_Scheduler_" + wname, true));
    
    srv.scheduleStatsReporting(Server.statsReporter(wname, this.scheduler), 5, true);
    
    this.removeEmptyBufsTask = srv.getScheduler().scheduleAtFixedRate(new Runnable()
    {
      public void run()
      {
        PartitionedBufferManager.this.removeEmptyBuffers();
      }
    }, 2L, 2L, TimeUnit.SECONDS);
  }
  
  public void shutdown()
  {
    super.shutdown();
    this.removeEmptyBufsTask.cancel(true);
    this.scheduler.shutdown();
    try
    {
      if (!this.scheduler.awaitTermination(5L, TimeUnit.SECONDS)) {
        this.scheduler.shutdownNow();
      }
    }
    catch (InterruptedException ie)
    {
      this.scheduler.shutdownNow();
    }
  }
  
  private void removeEmptyBuffers()
  {
    while (!this.emptyBuffers.isEmpty())
    {
      BufWindow buffer = (BufWindow)this.emptyBuffers.poll();
      if (buffer == null) {
        break;
      }
      if (buffer.canBeShutDown()) {
        if (this.bufLock.tryLock()) {
          try
          {
            if (buffer.canBeShutDown())
            {
              buffer.shutdown();
              this.buffers.remove(buffer.getPartKey(), buffer);
              this.removedPartitions += 1;
            }
          }
          finally
          {
            this.bufLock.unlock();
          }
        } else {
          this.emptyBuffers.offer(buffer);
        }
      }
    }
  }
  
  private Collection<BufWindow> getBuffers()
  {
    return this.buffers.values();
  }
  
  private void addNewRows(RecordKey key, IBatch keybatch)
  {
    this.bufLock.lock();
    try
    {
      BufWindow b = (BufWindow)this.buffers.get(key);
      if (b == null)
      {
        if (logger.isDebugEnabled()) {
          logger.debug("Creating new window partition for key " + key);
        }
        b = createBuffer(key);
        this.buffers.put(key, b);
        this.createdPartitions += 1;
      }
      b.addNewRows(keybatch);
    }
    finally
    {
      this.bufLock.unlock();
    }
  }
  
  void closeBuffers()
  {
    for (BufWindow b : getBuffers()) {
      b.shutdown();
    }
  }
  
  public void receiveImpl(Object linkID, ITaskEvent event)
    throws Exception
  {
    IBatch batch = event.batch();
    for (Object o : Batch.partition(this.keyFactory, batch).entrySet())
    {
      Map.Entry<RecordKey, IBatch> e = (Map.Entry)o;
      RecordKey key = (RecordKey)e.getKey();
      IBatch keybatch = (IBatch)e.getValue();
      addNewRows(key, keybatch);
    }
  }
  
  void dumpBuffer(PrintStream out)
  {
    for (BufWindow b : getBuffers()) {
      b.dumpBuffer(out);
    }
  }
  
  public void dumpActiveSnapshots(PrintStream out)
  {
    for (BufWindow b : getBuffers()) {
      b.dumpActiveSnapshots(out);
    }
  }
  
  public Range makeSnapshot()
  {
    Map<RecordKey, IBatch> map = new HashMap();
    for (BufWindow b : getBuffers())
    {
      Snapshot sn = b.getSnapshot();
      map.put(b.getPartKey(), Batch.asBatch(sn));
    }
    return (Range)Range.createRange(map);
  }
  
  public Position getCheckpoint(Window window)
  {
    Position result = null;
    String distId;
    for (BufWindow buffer : getBuffers())
    {
      Snapshot sn = buffer.getSnapshot();
      if (sn != null)
      {
        distId = buffer.getPartKey().toPartitionKey();
        for (WAEvent ev : sn) {
          if (ev.position != null)
          {
            if (result == null) {
              result = new Position();
            }
            Position augmentedPosition = ev.position.createAugmentedPosition(window.getMetaID(), distId);
            result.mergeLowerPositions(augmentedPosition);
          }
          else if (logger.isDebugEnabled())
          {
            logger.debug("Unexpected request for current window position but found null event position");
          }
        }
      }
    }
    
    if (logger.isDebugEnabled())
    {
      logger.debug("Partitioned Window " + window.getMetaName() + " checkpoint: " + result);
      if (result != null) {
        Utility.prettyPrint(result);
      }
    }
    return result;
  }
  
  public void flushAll()
  {
    Map<RecordKey, BufWindow> newbufs = new HashMap();
    for (BufWindow buffer : getBuffers())
    {
      buffer.shutdown();
      RecordKey pk = buffer.getPartKey();
      BufWindow b = createBuffer(pk);
      newbufs.put(pk, b);
    }
    this.bufLock.lock();
    try
    {
      this.buffers.clear();
      this.buffers.putAll(newbufs);
    }
    finally
    {
      this.bufLock.unlock();
    }
  }
  
  long prevSize = -1L;
  long prevHead = -1L;
  long prevTail = -1L;
  long prevNumPartitions = -1L;
  
  public void addSpecificMonitorEventsForWindow(Window window, MonitorEventsCollection monEvs)
  {
    Collection<BufWindow> bufs = getBuffers();
    if (bufs.isEmpty()) {
      return;
    }
    long size = 0L;
    long head = Long.MIN_VALUE;
    long tail = Long.MAX_VALUE;
    for (BufWindow b : bufs)
    {
      Snapshot s = b.getSnapshot();
      if (s != null) {
        size += s.size();
      }
    }
    long numPartitions = bufs.size();
    
    boolean showedActivity = false;
    if (this.prevSize != size)
    {
      monEvs.add(MonitorEvent.Type.WINDOW_SIZE, Long.valueOf(size));
      showedActivity = true;
    }
    if (this.prevNumPartitions != numPartitions)
    {
      monEvs.add(MonitorEvent.Type.NUM_PARTITIONS, Long.valueOf(numPartitions));
      showedActivity = true;
    }
    if (this.prevHead != head)
    {
      monEvs.add(MonitorEvent.Type.RANGE_HEAD, Long.valueOf(head));
      showedActivity = true;
    }
    if (this.prevTail != tail)
    {
      monEvs.add(MonitorEvent.Type.RANGE_TAIL, Long.valueOf(tail));
      showedActivity = true;
    }
    if (showedActivity) {
      monEvs.add(MonitorEvent.Type.LATEST_ACTIVITY, Long.valueOf(System.currentTimeMillis()));
    }
    this.prevSize = size;
    this.prevHead = head;
    this.prevTail = tail;
    this.prevNumPartitions = numPartitions;
  }
  
  void addEmptyBuffer(BufWindow buffer)
  {
    this.emptyBuffers.offer(buffer);
  }
  
  private static class PStats
    extends BufferManager.Stats
  {
    int received = 0;
    int numOfPartitions = 0;
    int numOfActiveSnapshots = 0;
    int size = 0;
    int logicalSize = 0;
    int createdPartitions = 0;
    int removedPartitions = 0;
    int numOfEmptyPartitions = 0;
    int numOfNonEmptyPartitions = 0;
    int numOfEmptyBufs = 0;
    
    public String toString()
    {
      return "received:" + this.received + "\npartitions: " + this.numOfPartitions + "(" + this.createdPartitions + "-" + this.removedPartitions + ")" + " snapshots: " + this.numOfActiveSnapshots + " treeElems: " + this.size + " size: " + this.logicalSize + "\nempty parts:" + this.numOfEmptyPartitions + " non-empty parts:" + this.numOfNonEmptyPartitions + " empty buffers:" + this.numOfEmptyBufs;
    }
  }
  
  public BufferManager.Stats getStats()
  {
    PStats s = new PStats();
    s.received = getInputCount();
    s.numOfPartitions = this.buffers.size();
    for (BufWindow w : getBuffers())
    {
      if (w != null) {
        if (w.snapshot.isEmpty()) {
          s.numOfEmptyPartitions += 1;
        } else {
          s.numOfNonEmptyPartitions += 1;
        }
      }
      s.logicalSize += w.getLogicalSize();
      s.numOfActiveSnapshots += w.numOfActiveSnapshots();
      s.size += w.numOfElements();
    }
    s.createdPartitions = this.createdPartitions;
    s.removedPartitions = this.removedPartitions;
    s.numOfEmptyBufs = this.emptyBuffers.size();
    return s;
  }
  
  ScheduledExecutorService getScheduler()
  {
    return this.scheduler;
  }
}
