package com.bloom.runtime.window;

import com.bloom.runtime.containers.Batch;
import com.bloom.runtime.containers.Range;

class SimpleBufferPolicy
  implements WBufferPolicy
{
  WBuffer buffer;
  
  public void updateBuffer(ScalaWindow w, Batch added, long now)
    throws Exception
  {
    w.updateBuffer(this.buffer, added, now);
  }
  
  public void initBuffer(ScalaWindow w)
  {
    this.buffer = w.createBuffer(null);
  }
  
  public void onJumpingTimer()
    throws Exception
  {
    this.buffer.removeAll();
  }
  
  public Range createSnapshot()
  {
    return this.buffer.makeSnapshot();
  }
}
