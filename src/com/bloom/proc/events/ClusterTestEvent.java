package com.bloom.proc.events;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.bloom.event.SimpleEvent;
import org.joda.time.DateTime;

public class ClusterTestEvent
  extends SimpleEvent
{
  private static final long serialVersionUID = -4518801128514134357L;
  public String keyValue;
  public DateTime ts;
  public int intValue;
  public double doubleValue;
  
  public ClusterTestEvent() {}
  
  public ClusterTestEvent(long timestamp)
  {
    super(timestamp);
  }
  
  public String getKeyValue()
  {
    return this.keyValue;
  }
  
  public void setKeyValue(String keyValue)
  {
    this.keyValue = keyValue;
  }
  
  public DateTime getTs()
  {
    return this.ts;
  }
  
  public void setTs(DateTime ts)
  {
    this.ts = ts;
  }
  
  public int getIntValue()
  {
    return this.intValue;
  }
  
  public void setIntValue(int intValue)
  {
    this.intValue = intValue;
  }
  
  public double getDoubleValue()
  {
    return this.doubleValue;
  }
  
  public void setDoubleValue(double doubleValue)
  {
    this.doubleValue = doubleValue;
  }
  
  public void setPayload(Object[] payload)
  {
    if (payload != null)
    {
      this.keyValue = ((String)payload[0]);
      this.ts = ((DateTime)payload[1]);
      if (payload[2] != null) {
        this.intValue = ((Integer)payload[2]).intValue();
      }
      if (payload[3] != null) {
        this.doubleValue = ((Double)payload[3]).doubleValue();
      }
    }
  }
  
  public Object[] getPayload()
  {
    return new Object[] { this.keyValue, this.ts, Integer.valueOf(this.intValue), Double.valueOf(this.doubleValue) };
  }
  
  public void write(Kryo kryo, Output output)
  {
    super.write(kryo, output);
    output.writeString(this.keyValue);
    kryo.writeClassAndObject(output, this.ts);
    output.writeInt(this.intValue);
    output.writeDouble(this.doubleValue);
  }
  
  public void read(Kryo kryo, Input input)
  {
    super.read(kryo, input);
    this.keyValue = input.readString();
    this.ts = ((DateTime)kryo.readClassAndObject(input));
    this.intValue = input.readInt();
    this.doubleValue = input.readDouble();
  }
}
