package com.bloom.runtime;

import com.lmax.disruptor.EventFactory;
import com.bloom.recovery.Position;
import com.bloom.runtime.containers.ITaskEvent;

public class StreamEventFactory
  implements EventFactory<EventContainer>
{
  public static ITaskEvent createStreamEvent(Object data)
  {
    return createStreamEvent(data, new Position());
  }
  
  public static ITaskEvent createStreamEvent(Object data, Position pos)
  {
    return new StreamTaskEvent(data, pos);
  }
  
  public EventContainer newInstance()
  {
    return new EventContainer();
  }
}
