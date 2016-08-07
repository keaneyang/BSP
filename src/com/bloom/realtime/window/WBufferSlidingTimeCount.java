package com.bloom.runtime.window;

import com.bloom.runtime.containers.Batch;
import com.bloom.runtime.containers.WAEvent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

class WBufferSlidingTimeCount
  extends WBufferSliding
{
  LinkedList<TimeIndexEntry> index = new LinkedList();
  
  void update(Batch added, long now)
    throws Exception
  {
    addEvents(added);
    this.index.add(new TimeIndexEntry(added.size(), now));
    List<WAEvent> removed = new ArrayList();
    while (countOverflow())
    {
      removeEvent(removed);
      TimeIndexEntry e = (TimeIndexEntry)this.index.peek();
      e.count -= 1;
      if (e.count == 0) {
        this.index.remove();
      }
    }
    publish(added, removed);
  }
  
  public void removeTill(long now)
    throws Exception
  {
    List<WAEvent> removed = new ArrayList();
    while (!this.index.isEmpty())
    {
      TimeIndexEntry e = (TimeIndexEntry)this.index.peek();
      if (notExpiredYet(e.createdTimestamp, now)) {
        break;
      }
      e = (TimeIndexEntry)this.index.remove();
      for (int i = 0; i < e.count; i++) {
        removeEvent(removed);
      }
    }
    publish(Collections.emptyList(), removed);
  }
}
