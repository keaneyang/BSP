package com.bloom.runtime.window;

import com.bloom.runtime.ServerServices;
import com.bloom.runtime.containers.Range;
import com.bloom.runtime.monitor.MonitorEvent;
import com.bloom.runtime.monitor.MonitorEventsCollection;
import com.bloom.runtime.monitor.MonitorEvent.Type;
import com.bloom.utility.Utility;
import com.bloom.uuid.UUID;
import com.bloom.recovery.Path;
import com.bloom.recovery.Position;
import com.bloom.recovery.SourcePosition;
import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.ITaskEvent;
import com.bloom.runtime.containers.WAEvent;

import java.io.PrintStream;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.log4j.Logger;

class NoPartBufferManager
  extends BufferManager
{
  private final ServerServices srv;
  private BufWindow buffer;
  private Position waitPosition = new Position();
  
  NoPartBufferManager(UUID windowid, BufWindowFactory windowDesc, ServerServices srv)
  {
    super(windowid, windowDesc, srv);
    this.srv = srv;
    this.buffer = createBuffer(null);
  }
  
  void closeBuffers()
  {
    this.buffer.shutdown();
  }
  
  public void receiveImpl(Object linkID, ITaskEvent event)
    throws Exception
  {
    IBatch batch = event.batch();
    this.buffer.addNewRows(batch);
  }
  
  protected boolean isBeforeWaitPosition(Position position)
  {
    if ((this.waitPosition == null) || (position == null)) {
      return false;
    }
    for (Path wactionPath : position.values()) {
      if (this.waitPosition.containsKey(wactionPath.getPathHash()))
      {
        SourcePosition sp = this.waitPosition.get(Integer.valueOf(wactionPath.getPathHash())).getSourcePosition();
        if (wactionPath.getSourcePosition().compareTo(sp) <= 0)
        {
          if (logger.isInfoEnabled()) {
            logger.info("Rejection Waction as duplicate -- position=" + position);
          }
          return true;
        }
        this.waitPosition = this.waitPosition.createPositionWithoutPath(wactionPath);
      }
    }
    if (this.waitPosition.isEmpty()) {
      this.waitPosition = null;
    }
    return false;
  }
  
  void dumpBuffer(PrintStream out)
  {
    this.buffer.dumpBuffer(out);
  }
  
  public void dumpActiveSnapshots(PrintStream out)
  {
    this.buffer.dumpActiveSnapshots(out);
  }
  
  public Range makeSnapshot()
  {
    Snapshot sn = this.buffer.getSnapshot();
    return Range.createRange(null, sn);
  }
  
  public Position getCheckpoint(Window window)
  {
    Position result = null;
    
    Snapshot sn = this.buffer.getSnapshot();
    if (sn != null) {
      for (WAEvent ev : sn) {
        if (ev.position != null)
        {
          if (result == null) {
            result = new Position();
          }
          Position augmentedPosition = ev.position.createAugmentedPosition(window.getMetaID(), null);
          result.mergeLowerPositions(augmentedPosition);
        }
      }
    }
    if (logger.isDebugEnabled())
    {
      logger.debug("Unpartitioned Window " + window.getMetaName() + " checkpoint: " + result);
      if (result != null) {
        Utility.prettyPrint(result);
      }
    }
    return result;
  }
  
  public void flushAll()
  {
    this.buffer.shutdown();
    this.buffer = createBuffer(null);
  }
  
  long prevSize = -1L;
  long prevHead = -1L;
  long prevTail = -1L;
  
  public void addSpecificMonitorEventsForWindow(Window window, MonitorEventsCollection monEvs)
  {
    if (this.buffer == null) {
      return;
    }
    long head = this.buffer.getHead();
    long tail = this.buffer.getTail();
    
    Snapshot s = this.buffer.getSnapshot();
    if (s != null)
    {
      long size = s.size();
      if (this.prevSize != size) {
        monEvs.add(MonitorEvent.Type.WINDOW_SIZE, Long.valueOf(size));
      }
      this.prevSize = size;
    }
    if (this.prevHead != head) {
      monEvs.add(MonitorEvent.Type.RANGE_HEAD, Long.valueOf(head));
    }
    if (this.prevTail != tail) {
      monEvs.add(MonitorEvent.Type.RANGE_TAIL, Long.valueOf(tail));
    }
    this.prevHead = head;
    this.prevTail = tail;
  }
  
  void addEmptyBuffer(BufWindow buffer) {}
  
  public BufferManager.Stats getStats()
  {
    return new BufferManager.Stats()
    {
      int received = NoPartBufferManager.this.getInputCount();
      int logicalSize = NoPartBufferManager.this.buffer.getLogicalSize();
      int numOfActiveSnapshots = NoPartBufferManager.this.buffer.numOfActiveSnapshots();
      int size = NoPartBufferManager.this.buffer.numOfElements();
      
      public String toString()
      {
        return "received:" + this.received + " snapshots: " + this.numOfActiveSnapshots + " treeElems: " + this.size + " size: " + this.logicalSize;
      }
    };
  }
  
  ScheduledExecutorService getScheduler()
  {
    return this.srv.getScheduler();
  }
}
