package com.bloom.proc.events;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.bloom.anno.EventType;
import com.bloom.anno.EventTypeData;
import com.bloom.event.SimpleEvent;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;

@EventType(schema="Internal", classification="All", uri="com.bloom.proc.events:AvroRecordEvent:1.0")
public class AvroRecordEvent
  extends SimpleEvent
{
  private static final long serialVersionUID = 8049296225793295226L;
  @EventTypeData
  public GenericRecord data;
  public Map<String, Object> metadata;
  
  public AvroRecordEvent() {}
  
  public AvroRecordEvent(long timestamp)
  {
    super(timestamp);
  }
  
  public GenericRecord getData()
  {
    return this.data;
  }
  
  public void setData(GenericRecord record)
  {
    this.data = record;
  }
  
  public void setPayload(Object[] payload)
  {
    this.data = ((GenericRecord)payload[0]);
  }
  
  public Object[] getPayload()
  {
    return new Object[] { this.data, this.metadata };
  }
  
  public void write(Kryo kryo, Output output)
  {
    super.write(kryo, output);
    kryo.writeClassAndObject(output, this.data);
    kryo.writeClassAndObject(output, this.metadata);
  }
  
  public void read(Kryo kryo, Input input)
  {
    super.read(kryo, input);
    this.data = ((GenericRecord)kryo.readClassAndObject(input));
    this.metadata = ((Map)kryo.readClassAndObject(input));
  }
}
