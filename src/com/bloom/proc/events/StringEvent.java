package com.bloom.proc.events;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.bloom.anno.EventType;
import com.bloom.anno.EventTypeData;
import com.bloom.event.SimpleEvent;

@EventType(schema="Internal", classification="All", uri="com.bloom.proc.events:StringEvent:1.0")
public class StringEvent
  extends SimpleEvent
{
  private static final long serialVersionUID = -861041374336281417L;
  @EventTypeData
  public String data;
  
  public StringEvent() {}
  
  public StringEvent(long timestamp)
  {
    super(timestamp);
  }
  
  public String getData()
  {
    return this.data;
  }
  
  public void setData(String data)
  {
    this.data = data;
  }
  
  public void setPayload(Object[] payload)
  {
    this.data = ((String)payload[0]);
  }
  
  public Object[] getPayload()
  {
    return new Object[] { this.data };
  }
  
  public void write(Kryo kryo, Output output)
  {
    super.write(kryo, output);
    output.writeString(this.data);
  }
  
  public void read(Kryo kryo, Input input)
  {
    super.read(kryo, input);
    this.data = input.readString();
  }
}
