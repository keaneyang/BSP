package com.bloom.kafkamessaging;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.bloom.recovery.Path;
import com.bloom.recovery.Position;

public class OffsetPosition
  extends Position
{
  private static final long serialVersionUID = -2092065497461398815L;
  private long offset;
  private long timestamp;
  
  public OffsetPosition(long offset, long timestamp)
  {
    this.offset = offset;
    this.timestamp = timestamp;
  }
  
  public OffsetPosition(OffsetPosition that)
  {
    super(that);
    this.offset = that.offset;
    this.timestamp = that.timestamp;
  }
  
  public long getOffset()
  {
    return this.offset;
  }
  
  public void setOffset(long offset)
  {
    this.offset = offset;
  }
  
  public long getTimestamp()
  {
    return this.timestamp;
  }
  
  public void setTimestamp(long timestamp)
  {
    this.timestamp = timestamp;
  }
  
  public void read(Kryo kryo, Input input)
  {
    super.read(kryo, input);
    this.offset = ((Long)kryo.readClassAndObject(input)).longValue();
  }
  
  public void write(Kryo kryo, Output output)
  {
    super.write(kryo, output);
    kryo.writeClassAndObject(output, Long.valueOf(this.offset));
  }
  
  public String toString()
  {
    StringBuilder result = new StringBuilder();
    
    result.append("[Offset=").append(this.offset).append("; Ts=").append(this.timestamp).append("; ");
    for (Path p : values())
    {
      result.append(p.toString());
      result.append(";");
    }
    result.append("]");
    
    return result.toString();
  }
}
