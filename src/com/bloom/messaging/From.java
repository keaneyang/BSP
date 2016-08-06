package com.bloom.messaging;

import com.bloom.uuid.UUID;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.Serializable;
import org.apache.commons.lang.builder.HashCodeBuilder;

public class From
  implements Serializable, KryoSerializable
{
  private static final long serialVersionUID = -5191939960746595129L;
  private UUID from;
  private String componentName;
  
  public From() {}
  
  public From(UUID from, String componentName)
  {
    this.from = from;
    this.componentName = componentName;
  }
  
  public UUID getFrom()
  {
    return this.from;
  }
  
  public String getComponentName()
  {
    return this.componentName;
  }
  
  public void setFrom(UUID from)
  {
    this.from = from;
  }
  
  public void setComponentName(String componentName)
  {
    this.componentName = componentName;
  }
  
  public void write(Kryo kryo, Output output)
  {
    kryo.writeObjectOrNull(output, this.from, UUID.class);
    output.writeString(this.componentName);
  }
  
  public void read(Kryo kryo, Input input)
  {
    this.from = ((UUID)kryo.readObjectOrNull(input, UUID.class));
    this.componentName = input.readString();
  }
  
  public String toString()
  {
    return this.from.toString() + ":" + this.componentName;
  }
  
  public boolean equals(From frm)
  {
    return (this.from.equals(frm.getFrom())) && (this.componentName.equals(frm.getComponentName()));
  }
  
  public int hashCode()
  {
    return new HashCodeBuilder().append(this.from.toString()).append(this.componentName).toHashCode();
  }
}
