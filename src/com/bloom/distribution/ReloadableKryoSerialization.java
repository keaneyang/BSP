package com.bloom.distribution;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInputStream;
import com.esotericsoftware.kryo.io.ByteBufferOutputStream;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.MapReferenceResolver;
import com.esotericsoftware.kryo.util.ObjectMap;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.FrameworkMessage;
import com.esotericsoftware.kryonet.FrameworkMessage.DiscoverHost;
import com.esotericsoftware.kryonet.FrameworkMessage.DiscoverReply;
import com.esotericsoftware.kryonet.FrameworkMessage.KeepAlive;
import com.esotericsoftware.kryonet.FrameworkMessage.Ping;
import com.esotericsoftware.kryonet.FrameworkMessage.RegisterTCP;
import com.esotericsoftware.kryonet.FrameworkMessage.RegisterUDP;
import com.esotericsoftware.kryonet.Serialization;
import java.nio.ByteBuffer;
import org.apache.log4j.Logger;

public class ReloadableKryoSerialization
  implements Serialization
{
  static Logger logger = Logger.getLogger(ReloadableKryoSerialization.class);
  private Kryo kryo;
  private ReloadableClassResolver resolver;
  private Object lock = new Object();
  private Input input;
  private Output output;
  private final ByteBufferInputStream byteBufferInputStream = new ByteBufferInputStream();
  private final ByteBufferOutputStream byteBufferOutputStream = new ByteBufferOutputStream();
  
  public ReloadableKryoSerialization()
  {
    reloadKryo();
    this.input = new Input(this.byteBufferInputStream, 512);
    this.output = new Output(this.byteBufferOutputStream, 512);
  }
  
  public void removeClassRegistration(Class<?> clazz)
  {
    this.resolver.removeClassRegistration(clazz);
  }
  
  public void addClassRegistration(Class<?> clazz, int id)
  {
    this.kryo.register(clazz, id);
  }
  
  public void reloadKryo()
  {
    synchronized (this.lock)
    {
      if (this.kryo == null)
      {
        this.resolver = new ReloadableClassResolver();
        this.kryo = new Kryo(this.resolver, new MapReferenceResolver());
      }
      else
      {
        this.kryo.reset();
      }
      this.kryo.register(FrameworkMessage.RegisterTCP.class);
      this.kryo.register(FrameworkMessage.RegisterUDP.class);
      this.kryo.register(FrameworkMessage.KeepAlive.class);
      this.kryo.register(FrameworkMessage.DiscoverHost.class);
      this.kryo.register(FrameworkMessage.DiscoverReply.class);
      this.kryo.register(FrameworkMessage.Ping.class);
    }
  }
  
  public Kryo getKryo()
  {
	  return kryo;
  }
  
  public void write(Connection connection, ByteBuffer buffer, Object object)
  {
    synchronized (this.lock)
    {
      try
      {
        synchronized (this.output)
        {
          this.byteBufferOutputStream.setByteBuffer(buffer);
          this.kryo.getContext().put("connection", connection);
          this.kryo.writeClassAndObject(this.output, object);
          this.output.flush();
        }
      }
      catch (Throwable t)
      {
        logger.error("Problem Writing Object", t);
      }
    }
  }
  
  public Object read(Connection connection, ByteBuffer buffer)
  {
    synchronized (this.lock)
    {
      try
      {
        synchronized (this.input)
        {
          this.byteBufferInputStream.setByteBuffer(buffer);
          this.kryo.getContext().put("connection", connection);
          return this.kryo.readClassAndObject(this.input);
        }
      }
      catch (Throwable t)
      {
        logger.error("Problem Reading Object", t);
        return null;
      }
    }
  }
  
  public void writeLength(ByteBuffer buffer, int length)
  {
    buffer.putInt(length);
  }
  
  public int readLength(ByteBuffer buffer)
  {
    return buffer.getInt();
  }
  
  public int getLengthLength()
  {
    return 4;
  }
}
