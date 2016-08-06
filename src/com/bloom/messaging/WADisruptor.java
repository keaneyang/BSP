package com.bloom.messaging;

import com.bloom.runtime.ChannelEventHandler;
import com.bloom.runtime.DistSub;
import com.bloom.runtime.EventContainer;
import com.bloom.runtime.StreamEventFactory;
import com.bloom.runtime.UnManagedZMQDistSub;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorVararg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.Executors;

public class WADisruptor
  implements InterThreadComm
{
  private Disruptor disruptor;
  private final WADisruptor.EventPublisherWithTranslator eventPublisher;
  WaitStrategy waitStrategy = getStrategy();
  Status status;
  
  public static enum Status
  {
    UNKNOWN,  RUNNING,  STOPPED;
    
    private Status() {}
  }
  
  public WADisruptor(int size, final int sendQueueId, final ChannelEventHandler channelEventHandler)
  {
    this.disruptor = new Disruptor(new StreamEventFactory(), size, Executors.newCachedThreadPool(), ProducerType.MULTI, this.waitStrategy);
    
    this.disruptor.handleEventsWith(new EventHandler[] { new EventHandler()
    {
      public void onEvent(Object event, long sequence, boolean endOfBatch)
        throws Exception
      {
        if ((event instanceof EventContainer))
        {
          EventContainer container = (EventContainer)event;
          channelEventHandler.sendEvent(container, sendQueueId);
        }
      }
    } });
    this.disruptor.start();
    this.eventPublisher = new EventPublisherWithTranslator(this.disruptor.getRingBuffer());
    this.status = Status.RUNNING;
  }
  
  public void put(Object waEvent, DistSub s, int partitionId, ChannelEventHandler channelEventHandler)
  {
    if (this.status == Status.RUNNING) {
      this.eventPublisher.onData(waEvent, s, partitionId, channelEventHandler);
    }
  }
  
  public long size()
  {
    return this.eventPublisher.size();
  }
  
  public void stop()
  {
    this.status = Status.STOPPED;
    this.disruptor.shutdown();
  }
  
  public WaitStrategy getStrategy()
  {
    String strategy = System.getProperty("com.bloom.config.waitStrategy");
    if (strategy != null)
    {
      if (strategy.equalsIgnoreCase("busyspin")) {
        return new BusySpinWaitStrategy();
      }
      if (strategy.equalsIgnoreCase("yield")) {
        return new YieldingWaitStrategy();
      }
      if (strategy.equalsIgnoreCase("sleep")) {
        return new SleepingWaitStrategy();
      }
      if (strategy.equalsIgnoreCase("block")) {
        return new BlockingWaitStrategy();
      }
      return new BlockingWaitStrategy();
    }
    return new BlockingWaitStrategy();
  }
  
  public class EventPublisherWithTranslator
  {
    private final RingBuffer<EventContainer> ringBuffer;
    
    public EventPublisherWithTranslator(RingBuffer ringBuffer)
    {
      this.ringBuffer = ringBuffer;
    }
    
    private final EventTranslatorVararg<EventContainer> TRANSLATOR2 = new EventTranslatorVararg<EventContainer>()
    {
      public void translateTo(EventContainer event, long sequence, Object... args)
      {
        event.setAllFields(args[0], (UnManagedZMQDistSub)args[1], ((Integer)args[2]).intValue(), (ChannelEventHandler)args[3]);
      }
    };
    
    public void onData(Object event, DistSub s, int partitionId, ChannelEventHandler channelEventHandler)
    {
      this.ringBuffer.publishEvent(this.TRANSLATOR2, new Object[] { event, s, Integer.valueOf(partitionId), channelEventHandler });
    }
    
    public long size()
    {
      return this.ringBuffer.remainingCapacity();
    }
  }
}
