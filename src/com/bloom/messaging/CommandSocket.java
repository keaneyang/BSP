package com.bloom.messaging;

import org.apache.log4j.Logger;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

public class CommandSocket
{
  private static Logger logger = Logger.getLogger(CommandSocket.class);
  public static final byte STOP = 1;
  private final ZContext context;
  private String endpoint;
  
  public CommandSocket()
  {
    this.context = null;
  }
  
  public CommandSocket(ZContext context)
  {
    this.context = context;
  }
  
  public void open(String endpoint)
  {
    this.endpoint = endpoint;
    if (logger.isDebugEnabled()) {
      logger.debug("Command socket for " + endpoint + " ready");
    }
  }
  
  public void sendCommand(byte cmd)
  {
    byte[] msg = new byte[1];
    msg[0] = cmd;
    ZMQ.Socket peer = this.context.createSocket(8);
    if (this.endpoint != null)
    {
      peer.connect(this.endpoint);
      peer.setDelayAttachOnConnect(true);
      peer.send(msg, 0);
      if (logger.isDebugEnabled()) {
        logger.debug("Sent control message to " + this.endpoint);
      }
    }
    else
    {
      logger.error("Can't close socket at: " + this.endpoint);
    }
    this.context.destroySocket(peer);
  }
}
