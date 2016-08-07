package com.bloom.runtime.channels;

import com.bloom.runtime.components.Link;
import com.bloom.runtime.components.Subscriber;
import com.bloom.runtime.monitor.MonitorEvent;
import com.bloom.runtime.containers.ITaskEvent;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

public class SimpleChannel
  implements Channel
{
  private Link sub = null;
  
  public void close()
    throws IOException
  {
    this.sub = null;
  }
  
  public Collection<MonitorEvent> getMonitorEvents(long ts)
  {
    return Collections.emptyList();
  }
  
  public void publish(ITaskEvent event)
    throws Exception
  {
    if (this.sub != null) {
      this.sub.subscriber.receive(Integer.valueOf(this.sub.linkID), event);
    }
  }
  
  public void addSubscriber(Link link)
  {
    if (this.sub != null) {
      throw new RuntimeException("SimpleChannel can have only one subscriber");
    }
    this.sub = link;
  }
  
  public void removeSubscriber(Link link)
  {
    if ((this.sub != null) && (this.sub.equals(link))) {
      this.sub = null;
    }
  }
  
  public void addCallback(Channel.NewSubscriberAddedCallback callback)
  {
    throw new RuntimeException("SimpleChannel cannot have callback");
  }
  
  public int getSubscribersCount()
  {
    return this.sub == null ? 0 : 1;
  }
}
