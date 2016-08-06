package com.bloom.proc.events;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.bloom.anno.EventType;
import com.bloom.anno.EventTypeData;
import com.bloom.event.SimpleEvent;

@EventType(schema="Internal", classification="All", uri="com.bloom.proc.events:ObjectArrayEvent:1.0")
public class ObjectArrayEvent
  extends SimpleEvent
{
  private static final long serialVersionUID = -861041374336281417L;
  @EventTypeData
  public Object[] data;
  
  public ObjectArrayEvent() {}
  
  public ObjectArrayEvent(long timestamp)
  {
    super(timestamp);
  }
  
  public Object[] getData()
  {
    return this.data;
  }
  
  public void setData(Object[] data)
  {
    this.data = ((Object[])data.clone());
  }
  
  public void setPayload(Object[] payload)
  {
    this.data = ((Object[])payload.clone());
  }
  
  public Object[] getPayload()
  {
    return this.data;
  }
  
  public void write(Kryo kryo, Output output)
  {
    super.write(kryo, output);
    if (this.data == null)
    {
      output.write(0);
    }
    else
    {
      output.write(1);
      output.writeInt(this.data.length);
      for (Object o : this.data) {
        kryo.writeClassAndObject(output, o);
      }
    }
  }
  
  public void read(Kryo kryo, Input input)
  {
    super.read(kryo, input);
    
    int nullOrNot = input.read();
    if (nullOrNot != 0)
    {
      int len = input.readInt();
      
      this.data = new Object[len];
      for (int i = 0; i < len; i++) {
        this.data[i] = kryo.readClassAndObject(input);
      }
    }
  }
}
