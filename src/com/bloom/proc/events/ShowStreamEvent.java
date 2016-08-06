package com.bloom.proc.events;

import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Stream;
import com.bloom.uuid.UUID;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.bloom.runtime.containers.WAEvent;

import java.io.Serializable;

public class ShowStreamEvent
  implements Serializable, KryoSerializable
{
  private static final long serialVersionUID = -4422795284888376503L;
  public String streamName;
  public UUID streamUUID;
  public WAEvent event;
  
  public ShowStreamEvent() {}
  
  public ShowStreamEvent(MetaInfo.Stream streamInfo, WAEvent event)
  {
    this.streamName = streamInfo.uri;
    this.streamUUID = streamInfo.uuid;
    this.event = event;
  }
  
  public String getStreamName()
  {
    return this.streamName;
  }
  
  public void setStreamName(String streamName)
  {
    this.streamName = streamName;
  }
  
  public UUID getStreamUUID()
  {
    return this.streamUUID;
  }
  
  public void setStreamUUID(UUID streamUUID)
  {
    this.streamUUID = streamUUID;
  }
  
  public WAEvent getEvent()
  {
    return this.event;
  }
  
  public void setEvent(WAEvent event)
  {
    this.event = event;
  }
  
  public void write(Kryo kryo, Output output)
  {
    output.writeString(this.streamName);
    if (this.streamUUID != null)
    {
      output.writeByte(0);
      kryo.writeClassAndObject(output, this.streamUUID);
    }
    else
    {
      output.writeByte(1);
    }
    if (this.event != null)
    {
      output.writeByte(0);
      kryo.writeClassAndObject(output, this.event);
    }
    else
    {
      output.writeByte(1);
    }
  }
  
  public void read(Kryo kryo, Input input)
  {
    this.streamName = input.readString();
    byte hasStreamUUID = input.readByte();
    if (hasStreamUUID == 0) {
      this.streamUUID = ((UUID)kryo.readClassAndObject(input));
    } else {
      this.streamUUID = null;
    }
    byte hasEvent = input.readByte();
    if (hasEvent == 0) {
      this.event = ((WAEvent)kryo.readClassAndObject(input));
    } else {
      this.event = null;
    }
  }
}
