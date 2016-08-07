package com.bloom.runtime.monitor;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.bloom.event.SimpleEvent;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class MonitorBatchEvent
  extends SimpleEvent
  implements Serializable
{
  private static final long serialVersionUID = 8522984791771979498L;
  
  public MonitorBatchEvent() {}
  
  public MonitorBatchEvent(long timestamp)
  {
    super(timestamp);
  }
  
  public MonitorBatchEvent(long timestamp, List<MonitorEvent> events)
  {
    super(timestamp);
    this.events = events;
  }
  
  public List<MonitorEvent> events = null;
  
  public void setPayload(Object[] payload)
  {
    if (payload != null)
    {
      this.events = new ArrayList(payload.length);
      for (Object obj : payload) {
        if ((obj instanceof MonitorEvent)) {
          this.events.add((MonitorEvent)obj);
        }
      }
    }
  }
  
  public Object[] getPayload()
  {
    return this.events == null ? null : this.events.toArray();
  }
  
  public void write(Kryo kryo, Output output)
  {
    super.write(kryo, output);
    kryo.writeClassAndObject(output, this.events);
  }
  
  public void read(Kryo kryo, Input input)
  {
    super.read(kryo, input);
    this.events = ((List)kryo.readClassAndObject(input));
  }
}
