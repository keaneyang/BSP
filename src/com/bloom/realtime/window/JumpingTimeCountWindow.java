package com.bloom.runtime.window;

import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.WAEvent;
import java.util.List;
import java.util.concurrent.Future;

class JumpingTimeCountWindow
  extends JumpingWindow
{
  private final long time_interval;
  private final int row_count;
  private Future<?> task;
  private volatile boolean cancelled = false;
  
  JumpingTimeCountWindow(BufferManager owner)
  {
    super(owner);
    this.time_interval = getPolicy().getTimeInterval().longValue();
    this.row_count = getPolicy().getRowCount().intValue();
    setNewWindowSnapshot(this.snapshot);
    this.task = schedule(this.time_interval);
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
    long tail = getTail();
    long nextTail = this.nextHead.longValue() + this.row_count;
    if (nextTail <= tail)
    {
      this.task.cancel(true);
      Snapshot newsn = makeSnapshot(this.nextHead.longValue(), nextTail);
      List<WAEvent> oldEntries = setNewWindowSnapshot(newsn);
      this.nextHead = Long.valueOf(nextTail);
      this.task.cancel(false);
      this.task = schedule(this.time_interval);
      notifyOnUpdate(newsn, oldEntries);
      this.snapshot = newsn;
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
    this.task = schedule(this.time_interval);
    notifyOnTimer(newsn, oldEntries);
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
