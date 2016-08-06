package com.bloom.jmqmessaging;

import com.bloom.messaging.Sender;
import com.bloom.messaging.SocketType;
import com.bloom.messaging.TransportMechanism;
import com.bloom.uuid.UUID;
import com.bloom.messaging.ReceiverInfo;

import java.util.Random;
import org.apache.log4j.Logger;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import zmq.ZError;
import zmq.ZError.IOException;

public abstract class ZMQSender
  extends Sender
{
  private static final Logger logger = Logger.getLogger(ZMQSender.class);
  final ZContext ctx;
  final ZContext shadowCtx;
  protected final boolean isEncrypted;
  private ZMQ.Socket socket = null;
  private final int sendBuffer = 1048576;
  private final int hwm = 50000;
  private final int linger = 0;
  private final int sendTimeOut = -1;
  private boolean hasStarted = false;
  private final SocketType type;
  private final Random rand = new Random(System.currentTimeMillis());
  private String identity = null;
  
  public ZMQSender(ZContext ctx, UUID serverID, ReceiverInfo info, SocketType type, TransportMechanism mechansim, boolean isEncrypted)
  {
    super(serverID, info, mechansim);
    this.isEncrypted = isEncrypted;
    this.ctx = ctx;
    this.shadowCtx = ZContext.shadow(this.ctx);
    this.type = type;
  }
  
  public ZMQ.Socket getSocket()
  {
    return this.socket;
  }
  
  public SocketType getType()
  {
    return this.type;
  }
  
  public void start()
    throws InterruptedException
  {
    try
    {
      if (this.hasStarted) {
        return;
      }
      if (getMechansim() == TransportMechanism.INPROC) {
        return;
      }
      switch (this.type)
      {
      case PUB: 
        this.socket = this.shadowCtx.createSocket(1);
        break;
      case SUB: 
        this.socket = this.shadowCtx.createSocket(2);
        break;
      case SYNCREQ: 
        this.socket = this.shadowCtx.createSocket(3);
        break;
      case ASYNCREQ: 
        this.socket = this.shadowCtx.createSocket(5);
        break;
      case SYNCREP: 
        this.socket = this.shadowCtx.createSocket(4);
        break;
      case ASYNCREP: 
        this.socket = this.shadowCtx.createSocket(6);
        break;
      case PUSH: 
        this.socket = this.shadowCtx.createSocket(8);
        break;
      case PULL: 
        this.socket = this.shadowCtx.createSocket(7);
      }
      setIdentity(this.socket);
      this.socket.setLinger(0L);
      this.socket.setSendBufferSize(1048576L);
      this.socket.setHWM(50000L);
      this.socket.setDelayAttachOnConnect(false);
      this.socket.setSendTimeOut(-1);
      switch (getMechansim())
      {
      case INPROC: 
        if (logger.isDebugEnabled()) {
          logger.debug("Trying to connect to : " + getInfo().getName() + " at " + getInfo().getInprocURI());
        }
        if (logger.isInfoEnabled()) {
          logger.info("Trying to connect to : " + getInfo().getName());
        }
        this.socket.connect(getInfo().getInprocURI());
        if (logger.isDebugEnabled()) {
          logger.debug("Connected to : " + getInfo().getName() + " at " + getInfo().getInprocURI());
        }
        if (logger.isInfoEnabled()) {
          logger.info("Connected to : " + getInfo().getName());
        }
        break;
      case IPC: 
        if (logger.isDebugEnabled()) {
          logger.debug("Trying to connect to : " + getInfo().getName() + " at " + getInfo().getIpcURI());
        }
        if (logger.isInfoEnabled()) {
          logger.info("Trying to connect to : " + getInfo().getName());
        }
        this.socket.connect(getInfo().getIpcURI());
        if (logger.isDebugEnabled()) {
          logger.debug("Connected to : " + getInfo().getName() + " at " + getInfo().getIpcURI());
        }
        if (logger.isInfoEnabled()) {
          logger.info("Connected to : " + getInfo().getName());
        }
        break;
      case TCP: 
        if (logger.isDebugEnabled()) {
          logger.debug("Trying to connect to : " + getInfo().getName() + " at " + getInfo().getTcpURI());
        }
        if (logger.isInfoEnabled()) {
          logger.info("Trying to connect to : " + getInfo().getName());
        }
        this.socket.connect(getInfo().getTcpURI());
        if (logger.isDebugEnabled()) {
          logger.debug("Connected to : " + getInfo().getName() + " at " + getInfo().getTcpURI());
        }
        if (logger.isInfoEnabled()) {
          logger.info("Connected to : " + getInfo().getName());
        }
        break;
      }
      this.hasStarted = true;
    }
    catch (Throwable e)
    {
      if ((e instanceof ZError.IOException)) {
        throw new InterruptedException(e.getMessage());
      }
      logger.error("Problem starting ZMQReceiver " + getInfo(), e);
    }
  }
  
  public void setIdentity(ZMQ.Socket sock)
  {
    this.identity = String.format("%08X-%08X", new Object[] { Integer.valueOf(this.rand.nextInt()), Integer.valueOf(this.rand.nextInt()) });
    sock.setIdentity(this.identity.getBytes(ZMQ.CHARSET));
  }
  
  public String getIdentity()
  {
    return this.identity;
  }
  
  public void stop()
  {
    this.ctx.destroySocket(this.socket);
    this.shadowCtx.destroy();
  }
  
  public abstract boolean send(Object paramObject);
}
