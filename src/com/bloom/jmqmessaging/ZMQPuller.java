package com.bloom.jmqmessaging;

import com.bloom.messaging.CommandSocket;
import com.bloom.messaging.Handler;
import com.bloom.ser.KryoSingleton;
import com.esotericsoftware.kryo.KryoException;

import java.nio.channels.ClosedSelectorException;
import org.apache.log4j.Logger;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

import zmq.ZError;
import zmq.ZError.IOException;

public class ZMQPuller
  implements Runnable
{
  private static Logger logger = Logger.getLogger(ZMQPuller.class);
  private PullReceiver pullReceiver;
  private ZMsg msg = null;
  private final Handler rcvr;
  public CommandSocket command;
  
  public ZMQPuller(PullReceiver pullReceiver, Handler rcvr, CommandSocket command)
  {
    this.pullReceiver = pullReceiver;
    this.rcvr = rcvr;
    this.command = command;
  }
  
  public void run()
  {
    if (logger.isInfoEnabled()) {
      logger.info(this.pullReceiver.getName() + " ready to receive data");
    }
    if (logger.isDebugEnabled()) {
      logger.debug(this.pullReceiver.getName() + " ready to receive msgs at : " + "\n 1. " + this.pullReceiver.getTcpAddress() + "\n 2. " + this.pullReceiver.getIpcAddress() + " \n 3. " + this.pullReceiver.getInProcAddress() + "\n" + "-------------------------------------------------------------------");
    }
    try
    {
      for (;;)
      {
        if (!Thread.interrupted()) {
          try
          {
            this.msg = ZMsg.recvMsg(this.pullReceiver.receiverSocket);
            if (this.msg == null)
            {
              logger.warn("Got a null message in " + this.pullReceiver.getName());
              if (Thread.interrupted()) {}
            }
            else
            {
              ZFrame frame = this.msg.pop();
              byte[] bytes = frame.getData();
              if (bytes != null) {
                if (bytes.length == 1)
                {
                  if (bytes[0] == 1)
                  {
                    if (logger.isDebugEnabled()) {
                      logger.debug("Received control message at(" + this.pullReceiver.getName() + ") : " + bytes[0]);
                    }
                  }
                }
                else {
                  try
                  {
                    this.rcvr.onMessage(KryoSingleton.read(bytes, this.pullReceiver.isEncrypted));
                  }
                  catch (KryoException e)
                  {
                    throw new RuntimeException("cannot deserialize message", e);
                  }
                }
              }
              frame.destroy();
            }
          }
          catch (ZError.IOException|ClosedSelectorException|ZMQException e)
          {
            logger.debug("Got ZMQException " + e.getMessage());
          }
          catch (KryoException e)
          {
            if (e.getMessage().contains("Buffer underflow")) {
              logger.debug("Got Kryoexception " + e.getMessage());
            } else {
              throw e;
            }
          }
          catch (com.bloom.exception.RuntimeInterruptedException e)
          {
            logger.warn("Got RuntimeInterruptedException " + e.getMessage());
          }
          catch (com.hazelcast.core.RuntimeInterruptedException e)
          {
            logger.warn("Got RuntimeInterruptedException " + e.getMessage());
          }
        }
      }
      logger.info("Receiver " + this.pullReceiver.getName() + " stopped!");
      try
      {
        this.pullReceiver.getCtx().destroySocket(this.pullReceiver.receiverSocket);
      }
      catch (ZError.IOException|ClosedSelectorException|ZMQException e)
      {
        logger.warn(e.getMessage());
      }
      return;
    }
    finally
    {
      logger.info("Receiver " + this.pullReceiver.getName() + " stopped!");
      try
      {
        this.pullReceiver.getCtx().destroySocket(this.pullReceiver.receiverSocket);
      }
      catch (ZError.IOException|ClosedSelectorException|ZMQException e)
      {
        logger.warn(e.getMessage());
      }
    }
  }
}
