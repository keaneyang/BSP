package com.bloom.runtime.window;

import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.WAEvent;
import java.util.List;

class SlidingCountWindow
  extends BufWindow
{
  private final int row_count;
  
  SlidingCountWindow(BufferManager owner)
  {
    super(owner);
    this.row_count = getPolicy().getRowCount().intValue();
  }
  
  protected void update(IBatch newEntries)
  {
    Snapshot newsn = makeSnapshot(this.snapshot, this.row_count);
    List<WAEvent> oldEntries = setNewWindowSnapshot(newsn);
    notifyOnUpdate(newEntries, oldEntries);
  }
}
