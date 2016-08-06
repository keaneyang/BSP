package com.bloom.jmqmessaging;

import com.bloom.messaging.Handler;
import com.bloom.messaging.SocketType;
import com.bloom.messaging.TransportMechanism;
import com.bloom.uuid.UUID;
import com.bloom.jmqmessaging.ZMQReceiverInfo;

import java.nio.channels.ClosedSelectorException;
import java.util.concurrent.RejectedExecutionException;
import org.apache.log4j.Logger;
import org.zeromq.ZContext;
import org.zeromq.ZMQException;

import zmq.ZError;
import zmq.ZError.IOException;

public class InprocAsyncSender
  extends AsyncSender
{
  private static final Logger logger = Logger.getLogger(InprocAsyncSender.class);
  private final Handler receiveHandler;
  
  public InprocAsyncSender(ZContext ctx, UUID serverID, ZMQReceiverInfo info, SocketType type, ZMQReceiver receiver)
  {
    super(ctx, serverID, info, type, TransportMechanism.INPROC, false);
    this.receiveHandler = receiver.getRcvr();
  }
  
  protected void cleanup() {}
  
  protected void destroySocket() {}
  
  protected void processMessage(Object data)
  {
    try
    {
      this.receiveHandler.onMessage(data);
    }
    catch (RejectedExecutionException e)
    {
      logger.warn("got exception for async sender thread " + e.getMessage());
    }
    catch (com.bloom.exception.RuntimeInterruptedException e)
    {
      logger.warn("Got RuntimeInterruptedException " + e.getMessage());
    }
    catch (ZError.IOException|ClosedSelectorException|ZMQException e)
    {
      logger.warn("Got ZMQException " + e.getMessage());
    }
    catch (com.hazelcast.core.RuntimeInterruptedException e)
    {
      logger.warn("Got RuntimeInterruptedException " + e.getMessage());
    }
    catch (RuntimeException e)
    {
      logger.warn("got exception for async sender thread " + e.getMessage());
    }
    catch (Exception e)
    {
      logger.warn("got exception for async sender thread " + e.getMessage());
    }
  }
}
