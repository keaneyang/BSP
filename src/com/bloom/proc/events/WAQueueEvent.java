package com.bloom.proc.events;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.Serializable;

public class WAQueueEvent
  implements Serializable, KryoSerializable
{
  private static final long serialVersionUID = 8283123723215896551L;
  String key;
  Object payload;
  
  public WAQueueEvent(String key, Object payload)
  {
    this.key = key;
    this.payload = payload;
  }
  
  public String getKey()
  {
    return this.key;
  }
  
  public void setKey(String key)
  {
    this.key = key;
  }
  
  public Object getPayload()
  {
    return this.payload;
  }
  
  public void setPayload(Object payload)
  {
    this.payload = payload;
  }
  
  public String toString()
  {
    return "WAQueueMsg(" + this.key + " " + ((this.payload instanceof byte[]) ? Integer.valueOf(((byte[])this.payload).length) : this.payload) + ")";
  }
  
  public void write(Kryo kryo, Output output)
  {
    output.writeString(this.key);
    if (this.payload != null)
    {
      output.writeByte(0);
      kryo.writeClassAndObject(output, this.payload);
    }
    else
    {
      output.writeByte(1);
    }
  }
  
  public void read(Kryo kryo, Input input)
  {
    this.key = input.readString();
    byte hasPayload = input.readByte();
    if (hasPayload == 0) {
      this.payload = kryo.readClassAndObject(input);
    }
  }
}
