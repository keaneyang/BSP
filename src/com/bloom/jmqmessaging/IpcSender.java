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

@Deprecated
public class IpcSender
  extends ZMQSender
{
  private static Logger logger = Logger.getLogger(IpcSender.class);
  
  public IpcSender(ZContext ctx, UUID serverID, ZMQReceiverInfo info, SocketType type, TransportMechanism mechanism, boolean isEncrypted)
  {
    super(ctx, serverID, info, type, mechanism, isEncrypted);
    if (logger.isDebugEnabled()) {
      logger.debug("IPC connection created for " + info.getName() + " on " + serverID);
    }
  }
  
  
  public boolean send(Object data)
  {
	  org.zeromq.ZMQ.Socket s = getSocket(); 
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
