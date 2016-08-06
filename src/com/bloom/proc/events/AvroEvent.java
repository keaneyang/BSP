package com.bloom.proc.events;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.bloom.anno.EventType;
import com.bloom.anno.EventTypeData;
import com.bloom.event.SimpleEvent;
import java.io.IOException;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;

@EventType(schema="Internal", classification="All", uri="com.bloom.proc.events:AvroEvent:1.0")
public class AvroEvent
  extends SimpleEvent
  implements JsonSerializable
{
  static Logger logger = Logger.getLogger(AvroEvent.class);
  private static final long serialVersionUID = 5544519789903788150L;
  @EventTypeData
  public GenericRecord data;
  public Map<String, Object> metadata;
  
  public AvroEvent() {}
  
  public AvroEvent(long timestamp)
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
    if (this.data == null)
    {
      output.writeByte((byte)0);
    }
    else
    {
      output.writeByte((byte)1);
      kryo.writeClassAndObject(output, this.data);
    }
    if (this.metadata == null)
    {
      output.writeByte((byte)0);
    }
    else
    {
      output.writeByte((byte)1);
      kryo.writeClassAndObject(output, this.metadata);
    }
  }
  
  public void read(Kryo kryo, Input input)
  {
    super.read(kryo, input);
    
    byte b = input.readByte();
    if (b != 0) {
      this.data = ((GenericRecord)kryo.readClassAndObject(input));
    }
    b = input.readByte();
    if (b != 0) {
      this.metadata = ((Map)kryo.readClassAndObject(input));
    }
  }
  
  public void serialize(JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
    throws IOException
  {
    jsonGenerator.writeStartObject();
    jsonGenerator.writeObjectField("data", this.data.toString());
    jsonGenerator.writeStringField("metadata", this.metadata.entrySet().toString());
    jsonGenerator.writeEndObject();
  }
  
  public void serializeWithType(JsonGenerator jsonGenerator, SerializerProvider serializerProvider, TypeSerializer typeSerializer)
    throws IOException
  {}
}
