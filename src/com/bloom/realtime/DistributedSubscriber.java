package com.bloom.runtime;

import com.bloom.runtime.DistLink;
import com.bloom.runtime.containers.ITaskEvent;

public abstract interface DistributedSubscriber
{
  public abstract void receive(DistLink paramDistLink, ITaskEvent paramITaskEvent);
}


