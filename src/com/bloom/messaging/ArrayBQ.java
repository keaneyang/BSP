package com.bloom.messaging;

import java.util.concurrent.BlockingQueue;
import org.jctools.queues.MpscCompoundQueue;

import com.bloom.runtime.ChannelEventHandler;
import com.bloom.runtime.DistSub;
import com.bloom.runtime.EventContainer;

public class ArrayBQ
  implements InterThreadComm
{
  private final BlockingQueue queue;
  private final ArrayBQ.MSGSender eventPublisher;
  private final ChannelEventHandler channelEventHandler;
  final int sendQueueId;
  Status status;
  
  public static enum Status
  {
    UNKNOWN,  RUNNING,  STOPPED;
    
    private Status() {}
  }
  
  public ArrayBQ(int size, int sendQueueId, ChannelEventHandler channelEventHandler)
  {
    this.queue = new MpscCompoundQueue(size);
    this.sendQueueId = sendQueueId;
    this.channelEventHandler = channelEventHandler;
    this.eventPublisher = new MSGSender(this.queue);
    this.eventPublisher.start();
    this.status = Status.RUNNING;
  }
  
  public void put(Object event, DistSub distSub, int partitionId, ChannelEventHandler channelEventHandler)
    throws InterruptedException
  {
    if (this.status == Status.RUNNING)
    {
      EventContainer container = new EventContainer();
      container.setAllFields(event, distSub, partitionId, channelEventHandler);
      this.queue.put(container);
    }
  }
  
  public long size()
  {
    return 0L;
  }
  
  public void stop()
  {
    this.status = Status.STOPPED;
    this.eventPublisher.interrupt();
    try
    {
      this.eventPublisher.join(10L);
    }
    catch (InterruptedException e)
    {
      e.printStackTrace();
    }
  }
  
  public class MSGSender
    extends Thread
  {
    private final BlockingQueue<EventContainer> eventQueue;
    
    MSGSender(BlockingQueue eventQueue)
    {
      this.eventQueue = eventQueue;
    }
    
    public void run()
    {
      try
      {
        while (!isInterrupted())
        {
          EventContainer container = (EventContainer)this.eventQueue.take();
          if (container != null) {
            ArrayBQ.this.channelEventHandler.sendEvent(container, ArrayBQ.this.sendQueueId);
          }
        }
      }
      catch (InterruptedException e)
      {
        e.printStackTrace();
      }
      catch (Exception e)
      {
        e.printStackTrace();
      }
    }
  }
}
