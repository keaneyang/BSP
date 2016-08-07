package com.bloom.runtime.window;

import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.WAEvent;
import java.util.List;

class JumpingCountWindow
  extends JumpingWindow
{
  private final int row_count;
  
  JumpingCountWindow(BufferManager owner)
  {
    super(owner);
    this.row_count = getPolicy().getRowCount().intValue();
  }
  
  protected void update(IBatch newEntries)
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
      Snapshot newsn = makeSnapshot(this.nextHead.longValue(), nextTail);
      List<WAEvent> oldEntries = setNewWindowSnapshot(newsn);
      this.nextHead = Long.valueOf(nextTail);
      notifyOnUpdate(newsn, oldEntries);
      this.snapshot = newsn;
    }
  }
}
