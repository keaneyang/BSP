package com.bloom.runtime.window;

import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.WAEvent;
import java.util.List;

class SlidingTimeCountWindow
  extends SlidingTimeWindow
{
  private final int row_count;
  
  SlidingTimeCountWindow(BufferManager owner)
  {
    super(owner);
    this.row_count = getPolicy().getRowCount().intValue();
  }
  
  protected synchronized void update(IBatch newEntries)
  {
    Snapshot newsn = makeSnapshot(this.snapshot, this.row_count);
    List<WAEvent> oldEntries = setNewWindowSnapshot(newsn);
    if (this.task == null) {
      this.task = schedule(this.time_interval);
    }
    notifyOnUpdate(newEntries, oldEntries);
  }
}
