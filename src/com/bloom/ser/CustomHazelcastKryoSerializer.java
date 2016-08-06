package com.bloom.ser;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.bloom.ser.KryoSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class CustomHazelcastKryoSerializer
  implements StreamSerializer<Object>
{
  private final Class clazz;
  private final int identifier;
  
  public CustomHazelcastKryoSerializer(Class clazzParam, int id)
  {
    this.clazz = clazzParam;
    this.identifier = id;
  }
  
  public int getTypeId()
  {
    return this.identifier;
  }
  
  public void destroy() {}
  
  public void write(ObjectDataOutput out, Object object)
    throws IOException
  {
    Kryo kryo = KryoSerializer.getSerializer();
    Output output = new Output((OutputStream)out);
    kryo.writeObject(output, object);
    output.flush();
  }
  
  public Object read(ObjectDataInput inputStream)
    throws IOException
  {
    InputStream in = (InputStream)inputStream;
    Input input = new Input(in);
    Kryo kryo = KryoSerializer.getSerializer();
    return kryo.readObject(input, this.clazz);
  }
}
