package com.bloom.runtime.components;

import com.bloom.runtime.containers.ITaskEvent;

public abstract interface Subscriber
{
  public abstract void receive(Object paramObject, ITaskEvent paramITaskEvent)
    throws Exception;
}
