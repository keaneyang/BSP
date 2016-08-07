package com.bloom.runtime;

public abstract interface ChannelEventHandler
{
  public abstract void sendEvent(EventContainer paramEventContainer, int paramInt)
    throws InterruptedException;
}
