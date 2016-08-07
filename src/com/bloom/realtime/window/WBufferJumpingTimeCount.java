package com.bloom.runtime.window;

import com.bloom.runtime.containers.Batch;
import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.WAEvent;

class WBufferJumpingTimeCount
  extends WBufferJumping
{
  void update(Batch added, long now)
    throws Exception
  {
    IBatch<WAEvent> waEventBatch = added;
    for (WAEvent e : waEventBatch)
    {
      addEvent(e);
      if (countFull()) {
        doJump();
      }
    }
  }
  
  void removeAll()
    throws Exception
  {
    doJump();
  }
}
