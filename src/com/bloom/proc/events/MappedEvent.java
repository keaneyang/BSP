package com.bloom.proc.events;

import com.bloom.uuid.UUID;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.bloom.event.SourceEvent;

import java.util.Map;

public class MappedEvent
  extends SourceEvent
{
  private static final long serialVersionUID = -739598948838180457L;
  public Map<String, Object> data;
  
  public MappedEvent() {}
  
  public MappedEvent(UUID sourceUUID)
  {
    super(sourceUUID);
  }
  
  public void write(Kryo kryo, Output output)
  {
    super.write(kryo, output);
    output.writeBoolean(this.data != null);
    if (this.data != null) {
      kryo.writeClassAndObject(output, this.data);
    }
  }
  
  public void read(Kryo kryo, Input input)
  {
    super.read(kryo, input);
    boolean dataNotNull = input.readBoolean();
    if (dataNotNull) {
      this.data = ((Map)kryo.readClassAndObject(input));
    } else {
      this.data = null;
    }
  }
}
