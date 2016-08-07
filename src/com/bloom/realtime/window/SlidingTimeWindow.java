package com.bloom.runtime.window;

import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.WAEvent;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

class SlidingTimeWindow
  extends BufWindow
{
  protected final long time_interval;
  protected volatile Future<?> task;
  private volatile boolean cancelled = false;
  
  SlidingTimeWindow(BufferManager owner)
  {
    super(owner);
    this.time_interval = getPolicy().getTimeInterval().longValue();
  }
  
  protected synchronized void update(IBatch newEntries)
  {
    long tail = getTail();
    Snapshot newsn = makeSnapshot(this.snapshot.vHead, tail);
    List<WAEvent> oldEntries = setNewWindowSnapshot(newsn);
    if (this.task == null) {
      this.task = getScheduler().schedule(this, this.time_interval, TimeUnit.NANOSECONDS);
    }
    notifyOnUpdate(newEntries, oldEntries);
  }
  
  protected synchronized void onTimer()
  {
    if (this.cancelled) {
      return;
    }
    WEntry eHead = null;
    long curtime = System.nanoTime();
    Iterator<WEntry> it = this.snapshot.itemIterator();
    while (it.hasNext())
    {
      WEntry e = (WEntry)it.next();
      if (e.timestamp + this.time_interval + 50000L > curtime)
      {
        eHead = e;
        break;
      }
    }
    long tail = getTail();
    long newHead = eHead == null ? tail : eHead.id;
    Snapshot newsn = makeSnapshot(newHead, tail);
    List<WAEvent> oldEntries = setNewWindowSnapshot(newsn);
    long leftToWait = eHead != null ? eHead.timestamp + this.time_interval - curtime : this.time_interval;
    
    this.task = schedule(leftToWait);
    if (!oldEntries.isEmpty()) {
      notifyOnTimer(Collections.emptyList(), oldEntries);
    }
  }
  
  void cancel()
  {
    if (this.task != null)
    {
      this.cancelled = true;
      this.task.cancel(true);
    }
    super.cancel();
  }
}
