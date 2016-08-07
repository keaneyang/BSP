package com.bloom.runtime.window;

import com.bloom.runtime.containers.Batch;
import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.WAEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

class WBufferSlidingAttr
  extends WBufferSliding
{
  void update(Batch added, long now)
    throws Exception
  {
    List<WAEvent> removed = new ArrayList();
    IBatch<WAEvent> waEventBatch = added;
    for (WAEvent ev : waEventBatch)
    {
      try
      {
        for (;;)
        {
          WAEvent firstInBuffer = getFirstEvent();
          if (inRange(firstInBuffer, ev)) {
            break;
          }
          removeEvent(removed);
        }
      }
      catch (NoSuchElementException e) {}
      addEvent(ev);
    }
    publish(added, removed);
  }
}
