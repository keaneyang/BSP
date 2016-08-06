package com.bloom.jmqmessaging;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.Serializable;

public class StreamInfoRequest
  implements KryoSerializable, Serializable
{
  private static final long serialVersionUID = 5676452220816985613L;
  private String streamName;
  
  public StreamInfoRequest() {}
  
  public StreamInfoRequest(String stream)
  {
    this.streamName = stream;
  }
  
  public String getStreamName()
  {
    return this.streamName;
  }
  
  public void write(Kryo kryo, Output output)
  {
    output.writeString(this.streamName);
  }
  
  public void read(Kryo kryo, Input input)
  {
    this.streamName = input.readString();
  }
  
  public String toString()
  {
    return "StreaminfoReqeust for " + this.streamName;
  }
}
