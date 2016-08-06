package com.bloom.proc.events;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.bloom.anno.EventType;
import com.bloom.anno.EventTypeData;
import com.bloom.event.SimpleEvent;
import flexjson.JSON;

@EventType(schema="Internal", classification="All", uri="com.bloom.proc.events:StringArrayEvent:1.0")
public class StringArrayEvent
  extends SimpleEvent
{
  private static final long serialVersionUID = -861041374336281417L;
  @EventTypeData
  public String[] data;
  
  public StringArrayEvent() {}
  
  public StringArrayEvent(long timestamp)
  {
    super(timestamp);
  }
  
  public String[] getData()
  {
    return this.data;
  }
  
  public void setData(String[] data)
  {
    this.data = ((String[])data.clone());
  }
  
  public void setPayload(Object[] payload)
  {
    this.data = ((String[])payload[0]);
  }
  
  @JSON(include=false)
  @JsonIgnore
  public Object[] getPayload()
  {
    return new Object[] { this.data };
  }
  
  public void write(Kryo kryo, Output output)
  {
    super.write(kryo, output);
    output.writeBoolean(this.data != null);
    if (this.data != null)
    {
      output.writeInt(this.data.length);
      for (String s : this.data) {
        output.writeString(s);
      }
    }
  }
  
  public void read(Kryo kryo, Input input)
  {
    super.read(kryo, input);
    boolean hasData = input.readBoolean();
    if (hasData)
    {
      int len = input.readInt();
      this.data = new String[len];
      for (int i = 0; i < len; i++) {
        this.data[i] = input.readString();
      }
    }
  }
}
