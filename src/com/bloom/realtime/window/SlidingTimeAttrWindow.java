package com.bloom.runtime.window;

import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.WAEvent;
import java.util.List;

class SlidingTimeAttrWindow
  extends SlidingTimeWindow
{
  private final CmpAttrs attrComparator;
  
  SlidingTimeAttrWindow(BufferManager owner)
  {
    super(owner);
    this.attrComparator = getPolicy().getComparator();
  }
  
  protected synchronized void update(IBatch newEntries)
  {
    if (!newEntries.isEmpty())
    {
      Snapshot newsn = makeNewAttrSnapshot(newEntries, this.attrComparator);
      List<WAEvent> oldEntries = setNewWindowSnapshot(newsn);
      if (this.task == null) {
        this.task = schedule(this.time_interval);
      }
      notifyOnUpdate(newEntries, oldEntries);
    }
  }
}
