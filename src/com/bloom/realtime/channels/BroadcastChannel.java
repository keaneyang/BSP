package com.bloom.runtime.channels;

import com.bloom.runtime.BaseServer;
import com.bloom.runtime.components.FlowComponent;
import com.bloom.runtime.components.Link;
import com.bloom.runtime.components.MonitorableComponent;
import com.bloom.runtime.components.Subscriber;
import com.bloom.runtime.containers.TaskEvent;
import com.bloom.runtime.monitor.MonitorEventsCollection;
import com.bloom.uuid.UUID;
import com.bloom.runtime.containers.ITaskEvent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class BroadcastChannel
  extends MonitorableComponent
  implements Channel
{
  private Channel.NewSubscriberAddedCallback callback;
  private final List<Link> links = new CopyOnWriteArrayList();
  private final FlowComponent owner;
  protected volatile long received = 0L;
  protected volatile long processed = 0L;
  
  public BroadcastChannel(FlowComponent owner)
  {
    this.owner = owner;
  }
  
  public void close()
    throws IOException
  {
    this.links.clear();
  }
  
  public void doPublish(ITaskEvent event)
    throws Exception
  {
    Iterator<Link> it = this.links.iterator();
    while (it.hasNext())
    {
      Link link = (Link)it.next();
      if ((this.owner != null) && (this.owner.recoveryIsEnabled()))
      {
        ITaskEvent sendEvent = TaskEvent.createStreamEventWithNewWAEvents(event);
        link.subscriber.receive(Integer.valueOf(link.linkID), sendEvent);
      }
      else
      {
        link.subscriber.receive(Integer.valueOf(link.linkID), event);
      }
    }
    this.processed += 1L;
  }
  
  public void publish(ITaskEvent event)
    throws Exception
  {
    this.received += 1L;
    doPublish(event);
  }
  
  public void addSubscriber(Link link)
  {
    if (this.callback != null) {
      this.callback.notifyMe(link);
    }
    this.links.add(link);
  }
  
  public void removeSubscriber(Link link)
  {
    this.links.remove(link);
  }
  
  public void addCallback(Channel.NewSubscriberAddedCallback callback)
  {
    this.callback = callback;
  }
  
  public int getSubscribersCount()
  {
    return this.links.size();
  }
  
  public void addSpecificMonitorEvents(MonitorEventsCollection events) {}
  
  public UUID getMetaID()
  {
    return this.owner.getMetaID();
  }
  
  public BaseServer srv()
  {
    return this.owner.srv();
  }
  
  public String getOwnerMetaName()
  {
    return this.owner.getMetaName();
  }
}
