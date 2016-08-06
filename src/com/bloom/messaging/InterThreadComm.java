package com.bloom.messaging;

import com.bloom.runtime.ChannelEventHandler;
import com.bloom.runtime.DistSub;

public abstract interface InterThreadComm
{
  public abstract void put(Object paramObject, DistSub paramDistSub, int paramInt, ChannelEventHandler paramChannelEventHandler)
    throws InterruptedException;
  
  public abstract long size();
  
  public abstract void stop();
}

