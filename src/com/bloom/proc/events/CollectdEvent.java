package com.bloom.proc.events;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.bloom.anno.EventType;
import com.bloom.anno.EventTypeData;
import com.bloom.event.SimpleEvent;
import org.joda.time.DateTime;

@EventType(schema="Internal", classification="All", uri="com.bloom.proc.events:CollectdEvent:1.0")
public class CollectdEvent
  extends SimpleEvent
{
  private static final long serialVersionUID = -4533854701582138286L;
  public String hostName;
  public DateTime time;
  public DateTime timeHighResolution;
  public String pluginName;
  public String pluginInstanceName;
  public String typeName;
  public String typeInstanceName;
  public Long timeInterval;
  public Long intervalHighResolution;
  public String message;
  public Long severity;
  @EventTypeData
  public Object[] data;
  
  public void setPayload(Object[] data)
  {
    this.data = ((Object[])data.clone());
  }
  
  public Object[] getPayload()
  {
    return new Object[] { this.data };
  }
  
  public void write(Kryo kryo, Output output)
  {
    super.write(kryo, output);
    output.writeString(this.hostName);
    kryo.writeClassAndObject(output, this.time);
    kryo.writeClassAndObject(output, this.timeHighResolution);
    output.writeString(this.pluginName);
    output.writeString(this.pluginInstanceName);
    output.writeString(this.typeName);
    output.writeString(this.typeInstanceName);
    output.writeLong(this.timeInterval.longValue());
    output.writeLong(this.intervalHighResolution.longValue());
    output.writeString(this.message);
    output.writeLong(this.severity.longValue());
  }
  
  public void read(Kryo kryo, Input input)
  {
    super.read(kryo, input);
    this.hostName = input.readString();
    this.time = ((DateTime)kryo.readClassAndObject(input));
    this.timeHighResolution = ((DateTime)kryo.readClassAndObject(input));
    this.pluginName = input.readString();
    this.pluginInstanceName = input.readString();
    this.typeName = input.readString();
    this.typeInstanceName = input.readString();
    this.timeInterval = Long.valueOf(input.readLong());
    this.intervalHighResolution = Long.valueOf(input.readLong());
    this.message = input.readString();
    this.severity = Long.valueOf(input.readLong());
  }
}
