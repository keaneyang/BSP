package com.bloom.runtime.window;

import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.WAEvent;
import java.util.List;

class SlidingAttributeWindow
  extends BufWindow
{
  private final CmpAttrs attrComparator;
  
  SlidingAttributeWindow(BufferManager owner)
  {
    super(owner);
    this.attrComparator = getPolicy().getComparator();
  }
  
  protected void update(IBatch newEntries)
  {
    if (!newEntries.isEmpty())
    {
      Snapshot newsn = makeNewAttrSnapshot(newEntries, this.attrComparator);
      List<WAEvent> oldEntries = setNewWindowSnapshot(newsn);
      notifyOnUpdate(newEntries, oldEntries);
    }
  }
}
