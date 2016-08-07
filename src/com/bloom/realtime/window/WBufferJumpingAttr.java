package com.bloom.runtime.window;

import com.bloom.runtime.containers.Batch;
import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.WAEvent;
import java.util.NoSuchElementException;

class WBufferJumpingAttr
  extends WBufferJumping
{
  void update(Batch added, long now)
    throws Exception
  {
    IBatch<WAEvent> waEventBatch = added;
    for (WAEvent ev : waEventBatch)
    {
      try
      {
        WAEvent firstInBuffer = getFirstEvent();
        if (!inRange(firstInBuffer, ev)) {
          doJump();
        }
      }
      catch (NoSuchElementException e) {}
      addEvent(ev);
    }
  }
}
