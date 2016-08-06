package com.bloom.jmqmessaging;

import com.bloom.messaging.SocketType;
import com.bloom.messaging.TransportMechanism;
import com.bloom.uuid.UUID;
import com.bloom.jmqmessaging.ZMQReceiverInfo;
import com.bloom.messaging.ReceiverInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import org.jctools.queues.MpscCompoundQueue;
import org.zeromq.ZContext;

public abstract class AsyncSender
  extends ZMQSender
{
  private static final Logger logger = Logger.getLogger(AsyncSender.class);
  private static final long QUEUE_OPERATION_WAIT_MS = 10L;
  private static int QUEUE_SIZE_MAX = 10000;
  private static final int MAX_MESSAGE_SEND = Integer.parseInt(System.getProperty("com.bloom.async.ASYNC_THRESHOLD", "50"));
  private static final boolean DO_POLL = Boolean.parseBoolean(System.getProperty("com.bloom.async.POLL", "true"));
  private static final boolean DO_OLD_PUBLISH = Boolean.parseBoolean(System.getProperty("com.bloom.async.SIMPLE", "false"));
  private static final int TIME_OUT = Integer.parseInt(System.getProperty("com.bloom.async.TIME_OUT", "50"));
  private final AsyncSenderThread senderThread;
  private final BlockingQueue<Object> messageQueue;
  List<Object> drainToMailbox = new ArrayList(MAX_MESSAGE_SEND + 1);
  
  public AsyncSender(ZContext ctx, UUID serverID, ZMQReceiverInfo info, SocketType type, TransportMechanism transportMechanism, boolean isEncrypted)
  {
    super(ctx, serverID, info, type, transportMechanism, isEncrypted);
    try
    {
      QUEUE_SIZE_MAX = Integer.parseInt(System.getProperty("com.bloom.optimalBackPressureThreshold", "10000"));
    }
    catch (Exception e)
    {
      logger.warn("Invalid setting for com.bloom.optimalBackPressureThreshold, defaulting to 10000");
      QUEUE_SIZE_MAX = 10000;
    }
    this.senderThread = new AsyncSenderThread();
    this.senderThread.setName(getInfo().getName() + ":Async-Sender");
    this.messageQueue = new MpscCompoundQueue(QUEUE_SIZE_MAX);
    logger.debug("Connection created for " + getInfo().getName() + " on " + serverID);
  }
  
  public void start()
    throws InterruptedException
  {
    logger.info("Starting Tcp/Inproc async Sender thread for sender " + getInfo().getName());
    super.start();
    this.messageQueue.clear();
    this.senderThread.start();
    reportSenderThreadStatus();
  }
  
  public void stop()
  {
    logger.info("Stopping Tcp/Inproc async Sender thread for sender " + this.senderThread.getName());
    this.senderThread.running = false;
    cleanup();
    try
    {
      this.senderThread.join(150L);
    }
    catch (InterruptedException ignored)
    {
      logger.warn("Failed to stop Tcp/Inproc async Sender thread for sender " + this.senderThread.getName());
    }
    this.messageQueue.clear();
    reportSenderThreadStatus();
  }
  
  protected abstract void cleanup();
  
  private void reportSenderThreadStatus()
  {
    logger.debug("Status of Sender Thread (" + this.senderThread.getName() + "): " + QUEUE_SIZE_MAX + ", " + this.senderThread.running);
  }
  
  protected abstract void destroySocket();
  
  protected abstract void processMessage(Object paramObject);
  
  private class AsyncSenderThread
    extends Thread
  {
    private volatile boolean running = true;
    
    private AsyncSenderThread() {}
    
    public void run()
    {
      while (this.running) {
        try
        {
          if (AsyncSender.DO_OLD_PUBLISH)
          {
            try
            {
              Object message = AsyncSender.this.messageQueue.poll(100L, TimeUnit.MILLISECONDS);
              if (message != null) {
                AsyncSender.this.processMessage(message);
              }
            }
            catch (Exception e)
            {
              AsyncSender.logger.error("Error publishing message", e);
            }
          }
          else if (AsyncSender.DO_POLL)
          {
            AsyncSender.this.drainToMailbox.clear();
            Object firstMessage = AsyncSender.this.messageQueue.poll(AsyncSender.TIME_OUT, TimeUnit.MILLISECONDS);
            if (firstMessage != null)
            {
              AsyncSender.this.drainToMailbox.add(firstMessage);
              AsyncSender.this.messageQueue.drainTo(AsyncSender.this.drainToMailbox, AsyncSender.MAX_MESSAGE_SEND);
              for (Object mail : AsyncSender.this.drainToMailbox) {
                AsyncSender.this.processMessage(mail);
              }
            }
          }
          else
          {
            AsyncSender.this.drainToMailbox.clear();
            AsyncSender.this.messageQueue.drainTo(AsyncSender.this.drainToMailbox, AsyncSender.MAX_MESSAGE_SEND);
            for (Object mail : AsyncSender.this.drainToMailbox) {
              AsyncSender.this.processMessage(mail);
            }
            if (AsyncSender.this.drainToMailbox.size() == 0)
            {
              Object firstMessage = AsyncSender.this.messageQueue.poll(AsyncSender.TIME_OUT, TimeUnit.MILLISECONDS);
              AsyncSender.this.processMessage(firstMessage);
            }
          }
        }
        catch (Exception e)
        {
          AsyncSender.logger.error("Error publishing message", e);
        }
      }
      AsyncSender.this.destroySocket();
      AsyncSender.logger.info("Exiting Tcp/Inproc async Sender thread for sender " + AsyncSender.this.senderThread.getName());
    }
  }
  
  public boolean send(Object message)
  {
    try
    {
      boolean sent = false;
      while ((!sent) && (this.senderThread.running)) {
        sent = this.messageQueue.offer(message, 100L, TimeUnit.MILLISECONDS);
      }
      if (!this.senderThread.running) {
        return false;
      }
    }
    catch (InterruptedException e)
    {
      logger.error("Thread interrupted");
    }
    return true;
  }
  
  public boolean isFull()
  {
    boolean result = this.messageQueue.size() >= QUEUE_SIZE_MAX - 2;
    return result;
  }
}
