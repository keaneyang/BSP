package com.bloom.runtime;

public class EventContainer
{
  private Object event;
  private DistSub distSub;
  private int partitionId;
  private ChannelEventHandler eventHandler;
  
  public EventContainer() {}
  
  public EventContainer(Object event, UnManagedZMQDistSub distSub, int partitionId, ChannelEventHandler channelEventHandler)
  {
    this.event = event;
    this.distSub = distSub;
    this.partitionId = partitionId;
    this.eventHandler = channelEventHandler;
  }
  
  public void setEventHandler(ChannelEventHandler eventHandler)
  {
    this.eventHandler = eventHandler;
  }
  
  public ChannelEventHandler getEventHandler()
  {
    return this.eventHandler;
  }
  
  public void setAllFields(Object event, DistSub distSub, int partitionId, ChannelEventHandler channelEventHandler)
  {
    setEvent(event);
    setEventHandler(channelEventHandler);
    setDistSub(distSub);
    setPartitionId(partitionId);
  }
  
  public void setEvent(Object event)
  {
    this.event = event;
  }
  
  public void setDistSub(DistSub distSub)
  {
    this.distSub = distSub;
  }
  
  public void setPartitionId(int partitionId)
  {
    this.partitionId = partitionId;
  }
  
  public Object getEvent()
  {
    return this.event;
  }
  
  public DistSub getDistSub()
  {
    return this.distSub;
  }
  
  public int getPartitionId()
  {
    return this.partitionId;
  }
}
