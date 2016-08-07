package com.bloom.runtime;

import com.bloom.runtime.containers.Batch;
import com.bloom.runtime.containers.TaskEvent;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.bloom.recovery.Position;
import com.bloom.runtime.containers.WAEvent;

public class StreamTaskEvent
  extends TaskEvent
{
  private static final long serialVersionUID = 8677542861922120067L;
  WAEvent xevent;
  
  public StreamTaskEvent() {}
  
  public StreamTaskEvent(Object data, Position pos)
  {
    this.xevent = new WAEvent(data, pos);
  }
  
  public Batch batch()
  {
    return Batch.asBatch(this.xevent);
  }
  
  public void write(Kryo kryo, Output output)
  {
    this.xevent.write(kryo, output);
  }
  
  public void read(Kryo kryo, Input input)
  {
    this.xevent = new WAEvent();
    this.xevent.read(kryo, input);
  }
}
