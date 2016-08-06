package com.bloom.proc.events;

import com.bloom.uuid.UUID;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.bloom.anno.EventType;
import com.bloom.anno.EventTypeData;
import com.bloom.event.SimpleEvent;

@EventType(schema="Internal", classification="All", uri="com.bloom.proc.events:DynamicEvent:1.0")
public class DynamicEvent
  extends SimpleEvent
{
  private static final long serialVersionUID = -7172644088275463812L;
  private UUID eventType;
  @EventTypeData
  public Object[] data;
  
  public DynamicEvent() {}
  
  public DynamicEvent(long timestamp)
  {
    super(timestamp);
  }
  
  public DynamicEvent(UUID eventType)
  {
    super(System.currentTimeMillis());
    this.eventType = eventType;
  }
  
  public void setData(String[] data)
  {
    this.data = ((Object[])data.clone());
  }
  
  public Object[] getData()
  {
    return this.data;
  }
  
  public UUID getEventType()
  {
    return this.eventType;
  }
  
  public void setPayload(Object[] data)
  {
    this.data = ((Object[])data[0]);
  }
  
  public Object[] getPayload()
  {
    return new Object[] { this.data };
  }
  
  public void write(Kryo kryo, Output output)
  {
    super.write(kryo, output);
    kryo.writeClassAndObject(output, this.eventType);
    kryo.writeClassAndObject(output, this.data);
  }
  
  public void read(Kryo kryo, Input input)
  {
    super.read(kryo, input);
    this.eventType = ((UUID)kryo.readClassAndObject(input));
    this.data = ((Object[])kryo.readClassAndObject(input));
  }
}
