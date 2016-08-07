package com.bloom.runtime.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.BlockingQueue;
import org.jctools.queues.MpscCompoundQueue;

public class NetLogger
{
  private AsynchronousSocketChannel socket;
  private final InetSocketAddress hostAddress;
  private final BlockingQueue<ByteBuffer> queue;
  private volatile boolean closed = false;
  
  private static class SingletonHolder
  {
    private static final PrintStream INSTANCE = NetLogger.create(55555);
  }
  
  public static PrintStream out()
  {
    return SingletonHolder.INSTANCE;
  }
  
  public static PrintStream create(int port)
  {
    return new NetLogger(port).createPrintStream();
  }
  
  private NetLogger(int port)
  {
    this.queue = new MpscCompoundQueue(1024);
    try
    {
      this.hostAddress = new InetSocketAddress(InetAddress.getByName("localhost"), port);
      this.socket = AsynchronousSocketChannel.open();
    }
    catch (IOException e)
    {
      throw new RuntimeException(e);
    }
    reconnect(ByteBuffer.allocate(0));
  }
  
  private void reconnect(ByteBuffer buf)
  {
    if (this.closed) {
      return;
    }
    if (!this.socket.isOpen()) {
      try
      {
        this.socket = AsynchronousSocketChannel.open();
      }
      catch (IOException e1)
      {
        e1.printStackTrace();
        return;
      }
    }
    this.socket.connect(this.hostAddress, buf, new CompletionHandler()
    {
      public void completed(Void result, ByteBuffer buf)
      {
        NetLogger.this.socket.write(buf, buf, new CompletionHandler()
        {
          public void completed(Integer result, ByteBuffer buf)
          {
            if (result.intValue() == -1)
            {
              NetLogger.this.reconnect(buf);
              return;
            }
            if (!buf.hasRemaining()) {
              try
              {
                buf = (ByteBuffer)NetLogger.this.queue.take();
              }
              catch (InterruptedException|IllegalArgumentException e)
              {
                return;
              }
            }
            NetLogger.this.socket.write(buf, buf, this);
          }
          
          public void failed(Throwable exc, ByteBuffer buf)
          {
            if ((exc instanceof IOException)) {
              try
              {
                NetLogger.this.socket.close();
              }
              catch (IOException e)
              {
                e.printStackTrace();
              }
            }
            NetLogger.this.reconnect(buf);
          }
        });
      }
      
      public void failed(Throwable e, ByteBuffer buf)
      {
        try
        {
          Thread.sleep(2000L);
        }
        catch (InterruptedException e1) {}
        NetLogger.this.reconnect(buf);
      }
    });
  }
  
  private PrintStream createPrintStream()
  {
    new PrintStream(new OutputStream()
    {
      final ByteArrayOutputStream pbuf = new ByteArrayOutputStream();
      
      public void write(int b)
        throws IOException
      {
        this.pbuf.write(b);
        if (b == 10)
        {
          NetLogger.this.queue.offer(ByteBuffer.wrap(this.pbuf.toByteArray()));
          this.pbuf.reset();
        }
      }
      
      public void close()
        throws IOException
      {
        NetLogger.this.closed = true;
        NetLogger.this.socket.close();
      }
    });
  }
}
