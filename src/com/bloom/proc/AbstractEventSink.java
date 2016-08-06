package com.bloom.proc;

import com.bloom.intf.EventSink;
import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.uuid.UUID;
import com.bloom.event.Event;
import com.bloom.recovery.Position;
import com.bloom.runtime.containers.ITaskEvent;

public abstract class AbstractEventSink
  implements EventSink
{
  public UUID getNodeID()
  {
    return HazelcastSingleton.getNodeId();
  }
  
  public void receive(int channel, Event event, Position pos)
    throws Exception
  {
    receive(channel, event);
  }
  
  public void receive(ITaskEvent batch)
    throws Exception
  {
    receive(0, null);
  }
}
