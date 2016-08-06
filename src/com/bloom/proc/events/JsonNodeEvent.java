package com.bloom.proc.events;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.bloom.anno.EventType;
import com.bloom.anno.EventTypeData;
import com.bloom.event.SimpleEvent;
import java.util.Map;

@EventType(schema="Internal", classification="All", uri="com.bloom.proc.events:JsonNodeEvent:1.0")
public class JsonNodeEvent
  extends SimpleEvent
{
  private static final long serialVersionUID = -861041374336281417L;
  @EventTypeData
  public JsonNode data;
  public Map<String, Object> metadata;
  
  public JsonNodeEvent() {}
  
  public JsonNodeEvent(long timestamp)
  {
    super(timestamp);
  }
  
  public JsonNode getData()
  {
    return this.data;
  }
  
  public void setData(JsonNode data)
  {
    this.data = data;
  }
  
  public void setPayload(Object[] payload)
  {
    this.data = ((JsonNode)payload[0]);
  }
  
  public Object[] getPayload()
  {
    return new Object[] { this.data, this.metadata };
  }
  
  public void write(Kryo kryo, Output output)
  {
    super.write(kryo, output);
    String dataVal = null;
    try
    {
      dataVal = jsonMapper.writeValueAsString(this.data);
    }
    catch (Exception e)
    {
      throw new RuntimeException("Could not serialize json data: " + this.data, e);
    }
    output.writeString(dataVal);
    kryo.writeClassAndObject(output, this.metadata);
  }
  
  public void read(Kryo kryo, Input input)
  {
    super.read(kryo, input);
    String dataVal = input.readString();
    if (dataVal != null) {
      try
      {
        this.data = jsonMapper.readTree(dataVal);
      }
      catch (Exception e)
      {
        throw new RuntimeException("Could not deserialize json data: " + dataVal, e);
      }
    }
    this.metadata = ((Map)kryo.readClassAndObject(input));
  }
}
