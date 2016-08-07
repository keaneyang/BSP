package com.bloom.runtime.window;

import com.bloom.runtime.containers.Batch;
import com.bloom.runtime.containers.WAEvent;
import java.util.ArrayList;
import java.util.List;

class WBufferSlidingCount
  extends WBufferSliding
{
  void update(Batch added, long now)
    throws Exception
  {
    addEvents(added);
    List<WAEvent> removed = new ArrayList();
    while (countOverflow()) {
      removeEvent(removed);
    }
    publish(added, removed);
  }
}
