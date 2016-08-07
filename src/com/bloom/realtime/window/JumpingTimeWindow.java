package com.bloom.runtime.window;

import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.WAEvent;
import java.util.List;
import java.util.concurrent.Future;

class JumpingTimeWindow
  extends JumpingWindow
{
  protected final long time_interval;
  private Future<?> task;
  private volatile boolean cancelled = false;
  
  JumpingTimeWindow(BufferManager owner)
  {
    super(owner);
    this.time_interval = getPolicy().getTimeInterval().longValue();
    this.nextHead = Long.valueOf(this.snapshot.vHead);
    setNewWindowSnapshot(this.snapshot);
    this.task = scheduleAtFixedRate(this.time_interval);
  }
  
  protected synchronized void update(IBatch newEntries)
  {
    if (this.nextHead == null)
    {
      this.nextHead = findHead();
      if (this.nextHead == null) {
        return;
      }
      this.snapshot = makeOneItemSnapshot(this.nextHead.longValue());
    }
  }
  
  protected synchronized void onTimer()
  {
    if (this.cancelled) {
      return;
    }
    if (this.nextHead == null)
    {
      this.nextHead = findHead();
      if (this.nextHead == null) {
        return;
      }
      this.snapshot = makeOneItemSnapshot(this.nextHead.longValue());
    }
    long tail = getTail();
    Snapshot newsn = makeSnapshot(this.nextHead.longValue(), tail);
    this.nextHead = Long.valueOf(tail);
    List<WAEvent> oldEntries = setNewWindowSnapshot(newsn);
    if ((!newsn.isEmpty()) || (!oldEntries.isEmpty())) {
      notifyOnTimer(newsn, oldEntries);
    }
    this.snapshot = newsn;
  }
  
  void cancel()
  {
    if (this.task != null)
    {
      this.cancelled = true;
      this.task.cancel(false);
    }
    super.cancel();
  }
}
