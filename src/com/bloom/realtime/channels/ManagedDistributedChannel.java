package com.bloom.runtime.channels;

import com.bloom.runtime.BaseServer;
import com.bloom.runtime.components.Link;
import com.bloom.runtime.components.Stream;
import com.bloom.runtime.monitor.MonitorEvent;
import com.bloom.runtime.containers.ITaskEvent;

import java.io.IOException;
import java.util.Collection;

public class ManagedDistributedChannel
  implements Channel
{
  private final BaseServer server;
  private final Stream stream_runtime;
  
  public ManagedDistributedChannel(BaseServer server, Stream stream_runtime)
  {
    this.server = server;
    this.stream_runtime = stream_runtime;
  }
  
  public void publish(ITaskEvent event)
    throws Exception
  {}
  
  public void addSubscriber(Link link) {}
  
  public void removeSubscriber(Link link) {}
  
  public void addCallback(Channel.NewSubscriberAddedCallback callback) {}
  
  public int getSubscribersCount()
  {
    return 0;
  }
  
  public void close()
    throws IOException
  {}
  
  public Collection<MonitorEvent> getMonitorEvents(long ts)
  {
    return null;
  }
}
