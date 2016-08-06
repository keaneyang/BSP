package com.bloom.intf;

import com.bloom.uuid.UUID;
import com.bloom.event.Event;
import com.bloom.recovery.Position;
import com.bloom.runtime.containers.ITaskEvent;

public abstract interface EventSink
{
  public abstract void receive(int paramInt, Event paramEvent)
    throws Exception;
  
  public abstract void receive(int paramInt, Event paramEvent, Position paramPosition)
    throws Exception;
  
  public abstract void receive(ITaskEvent paramITaskEvent)
    throws Exception;
  
  public abstract UUID getNodeID();
}

