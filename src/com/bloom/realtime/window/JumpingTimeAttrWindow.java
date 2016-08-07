package com.bloom.runtime.window;

import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.WAEvent;
import java.util.List;
import java.util.concurrent.Future;

class JumpingTimeAttrWindow
  extends JumpingWindow
{
  private final long time_interval;
  private final CmpAttrs attrComparator;
  private Future<?> task;
  private volatile boolean cancelled = false;
  
  JumpingTimeAttrWindow(BufferManager owner)
  {
    super(owner);
    this.time_interval = getPolicy().getTimeInterval().longValue();
    this.attrComparator = getPolicy().getComparator();
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
    long tail;
    WAEvent first;
    int n;
    if (!newEntries.isEmpty())
    {
      tail = getTail();
      first = getData(this.nextHead.longValue());
      assert (first != null);
      n = newEntries.size();
      for (Object o : newEntries)
      {
        WAEvent obj = (WAEvent)o;
        if (!this.attrComparator.inRange(first, obj))
        {
          long nextTail = tail - n;
          this.task.cancel(true);
          Snapshot newsn = makeSnapshot(this.nextHead.longValue(), nextTail);
          List<WAEvent> oldEntries = setNewWindowSnapshot(newsn);
          this.nextHead = Long.valueOf(nextTail);
          this.task.cancel(false);
          this.task = schedule(this.time_interval);
          notifyOnUpdate(newsn, oldEntries);
          this.snapshot = newsn;
          first = obj;
        }
        n--;
      }
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
