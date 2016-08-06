package com.bloom.jmqmessaging;

import java.util.Map;
import org.apache.log4j.Logger;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import com.bloom.messaging.CommandSocket;
import com.bloom.messaging.Handler;

import org.zeromq.ZMQException;

public class PullReceiver
  extends ZMQReceiver
{
  private static Logger logger = Logger.getLogger(PullReceiver.class);
  protected ZMQ.Socket receiverSocket;
  private final int HWM = 50000;
  private final int bufferSize = 1048576;
  private final int linger = 0;
  private Thread puller;
  private CommandSocket command;
  
  public PullReceiver(ZContext ctx, Handler rcvr, String name, boolean encrypted)
  {
    super(ctx, rcvr, name, encrypted);
    this.command = new CommandSocket(ctx);
    this.puller = new Thread(new ZMQPuller(this, rcvr, this.command));
    this.puller.setName(name + "-rcvr");
  }
  
  public void start(Map<Object, Object> properties)
    throws Exception
  {
    if (this.receiverSocket != null) {
      return;
    }
    this.receiverSocket = getCtx().createSocket(7);
    this.receiverSocket.setRcvHWM(50000L);
    this.receiverSocket.setLinger(0L);
    this.receiverSocket.setReceiveBufferSize(1048576L);
    try
    {
      int tcp = this.receiverSocket.bind(getTcpAddress());
      if (tcp == -1) {
        throw new Exception("Did not bind to appropriate TCP Address");
      }
      super.setTcpPort(tcp);
    }
    catch (ZMQException e)
    {
      logger.error(getName() + " was trying to bind to: " + getTcpAddress() + " but, failed", e);
      throw e;
    }
    try
    {
      int ipc = this.receiverSocket.bind(getIpcAddress());
      if (ipc == -1) {
        throw new Exception(getIpcAddress() + " is already in use!");
      }
      super.setIpcPort(ipc);
    }
    catch (ZMQException e)
    {
      logger.error(getName() + " was trying to bind to: " + getIpcAddress() + " but, failed", e);
      throw e;
    }
    try
    {
      int inproc = this.receiverSocket.bind(getInProcAddress());
      if (inproc == -1) {
        throw new Exception(getInProcAddress() + " is already is use!");
      }
      makeAddress();
      if (getInProcAddress() != null) {
        this.command.open(getInProcAddress());
      } else if (logger.isDebugEnabled()) {
        logger.debug("In Proc Address can't be NULL for: " + getName());
      }
      this.puller.start();
    }
    catch (ZMQException e)
    {
      logger.error(getName() + " was trying to bind to: " + getInProcAddress() + " but, failed", e);
      throw e;
    }
  }
  
  public boolean stop()
    throws Exception
  {
    logger.info("Stopping Receiver : " + getName());
    if (this.puller.isAlive()) {
      this.command.sendCommand((byte)1);
    }
    while (this.puller.isAlive())
    {
      this.puller.join(500L);
      if (logger.isDebugEnabled()) {
        logger.debug("Receiver : " + getName() + " is still alive!");
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Receiver : " + getName() + " isAlive? " + this.puller.isAlive());
    }
    this.receiverSocket = null;
    return this.puller.isAlive();
  }
}
