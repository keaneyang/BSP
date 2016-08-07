package com.bloom.runtime.window;

import com.bloom.runtime.containers.Batch;

class WBufferJumpingTime
  extends WBufferJumping
{
  void update(Batch added, long now)
    throws Exception
  {
    addEvents(added);
  }
  
  void removeAll()
    throws Exception
  {
    doJump();
  }
}
