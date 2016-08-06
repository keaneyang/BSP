package com.bloom.jmqmessaging;

import com.bloom.messaging.Handler;
import com.bloom.messaging.SocketType;
import com.bloom.messaging.TransportMechanism;
import com.bloom.uuid.UUID;
import com.bloom.jmqmessaging.ZMQReceiverInfo;

import org.apache.log4j.Logger;
import org.zeromq.ZContext;

public class InprocSender
  extends ZMQSender
{
  private static Logger logger = Logger.getLogger(InprocSender.class);
  private final ZMQReceiver msgr;
  
  public InprocSender(ZContext ctx, UUID serverID, ZMQReceiverInfo info, SocketType type, ZMQReceiver msgr)
  {
    super(ctx, serverID, info, type, TransportMechanism.INPROC, false);
    this.msgr = msgr;
    if (logger.isDebugEnabled()) {
      logger.debug("Connection created for " + info.getName() + " on " + serverID);
    }
  }
  
  public void start()
    throws InterruptedException
  {}
  
  public void stop() {}
  
  public boolean send(Object data)
  {
    this.msgr.getRcvr().onMessage(data);
    return true;
  }
  
  public boolean isFull()
  {
    return false;
  }
}
