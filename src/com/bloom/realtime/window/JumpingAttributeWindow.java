package com.bloom.runtime.window;

import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.WAEvent;
import java.util.List;

class JumpingAttributeWindow
  extends JumpingWindow
{
  private final CmpAttrs attrComparator;
  
  JumpingAttributeWindow(BufferManager owner)
  {
    super(owner);
    this.attrComparator = getPolicy().getComparator();
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
          Snapshot newsn = makeSnapshot(this.nextHead.longValue(), nextTail);
          List<WAEvent> oldEntries = setNewWindowSnapshot(newsn);
          this.nextHead = Long.valueOf(nextTail);
          notifyOnUpdate(newsn, oldEntries);
          this.snapshot = newsn;
          first = obj;
        }
        n--;
      }
    }
  }
}
