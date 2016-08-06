package com.bloom.jmqmessaging;

import com.bloom.messaging.SocketType;
import com.bloom.messaging.TransportMechanism;
import com.bloom.ser.KryoSingleton;
import com.bloom.uuid.UUID;
import com.bloom.jmqmessaging.ZMQReceiverInfo;

import org.apache.log4j.Logger;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQException;

public class TcpAsyncSender
  extends AsyncSender
{
  private static Logger logger = Logger.getLogger(TcpAsyncSender.class);
  
  public TcpAsyncSender(ZContext ctx, UUID serverID, ZMQReceiverInfo info, SocketType type, TransportMechanism mechansim, boolean isEncrypted)
  {
    super(new ZContext(1), serverID, info, type, mechansim, isEncrypted);
    if (logger.isDebugEnabled()) {
      logger.debug("TCP connection created for " + info.getName() + " on " + serverID);
    }
  }
  
  protected void cleanup()
  {
    this.ctx.destroy();
  }
  
  protected void destroySocket()
  {
    this.shadowCtx.destroySocket(getSocket());
  }
  
  protected void processMessage(Object data)
  {
    ZMQ.Socket s = getSocket();
    byte[] serializedData = KryoSingleton.write(data, this.isEncrypted);
    synchronized (s)
    {
      try
      {
        boolean success = s.send(serializedData);
        if (!success) {
          logger.warn("Failed to send message on stream");
        }
      }
      catch (ZMQException ex)
      {
        if (ex.getErrorCode() == 156384765) {
          logger.info(ex.getMessage());
        }
      }
    }
  }
  
  public boolean isFull()
  {
    ZMQ.Socket s = getSocket();
    int events = s.getEvents();
    
    boolean result = events == 0;
    return result;
  }
}
