package com.bloom.runtime.window;

import com.bloom.runtime.containers.Batch;
import com.bloom.runtime.containers.Range;

abstract interface WBufferPolicy
{
  public abstract void updateBuffer(ScalaWindow paramScalaWindow, Batch paramBatch, long paramLong)
    throws Exception;
  
  public abstract void initBuffer(ScalaWindow paramScalaWindow);
  
  public abstract void onJumpingTimer()
    throws Exception;
  
  public abstract Range createSnapshot();
}

