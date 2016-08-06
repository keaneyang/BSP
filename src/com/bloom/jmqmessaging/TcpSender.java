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

public class TcpSender
  extends ZMQSender
{
  private static Logger logger = Logger.getLogger(TcpSender.class);
  
  public TcpSender(ZContext ctx, UUID serverID, ZMQReceiverInfo info, SocketType type, TransportMechanism mechansim, boolean isEncrypted)
  {
    super(ctx, serverID, info, type, mechansim, isEncrypted);
    if (logger.isDebugEnabled()) {
      logger.debug("TCP connection created for " + info.getName() + " on " + serverID);
    }
  }
  
 
  public boolean send(Object data)
  {
	  ZMQ.Socket s = getSocket(); 
	  byte serializedData[] = KryoSingleton.write(data, isEncrypted); 
	  return s.send(serializedData);
	  
  }
  
  public boolean isFull()
  {
    ZMQ.Socket s = getSocket();
    int events = s.getEvents();
    
    boolean result = events == 0;
    return result;
  }
}
