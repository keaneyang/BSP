package com.bloom.ser;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DefaultSerializers;
import com.hazelcast.nio.serialization.DefaultSerializers.ObjectSerializer;
import com.hazelcast.nio.serialization.ObjectDataInputStream;
import com.hazelcast.nio.serialization.ObjectDataOutputStream;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationServiceBuilder;
import com.hazelcast.nio.serialization.Serializer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public final class HazelcastSerializer
{
  private static final transient SerializationService serService = new SerializationServiceBuilder().build();
  private static final transient ThreadLocal<Serializer> threadSerializers = new ThreadLocal()
  {
    protected Serializer initialValue()
    {
      return new DefaultSerializers.ObjectSerializer(true, false);
    }
  };
  
  public static Serializer getSerializer()
  {
    return (Serializer)threadSerializers.get();
  }
  
  public static byte[] write(Object obj)
  {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectDataOutput out = new ObjectDataOutputStream(baos, serService);
    try
    {
      ((DefaultSerializers.ObjectSerializer)getSerializer()).write(out, obj);
      return baos.toByteArray();
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    return null;
  }
  
  public static Object read(byte[] bytes)
  {
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    ObjectDataInput in = new ObjectDataInputStream(bais, serService);
    try
    {
      return ((DefaultSerializers.ObjectSerializer)getSerializer()).read(in);
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    return null;
  }
}
