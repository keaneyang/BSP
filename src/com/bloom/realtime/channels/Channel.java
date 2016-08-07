package com.bloom.runtime.channels;

import com.bloom.runtime.components.Link;
import com.bloom.runtime.monitor.Monitorable;
import com.bloom.runtime.containers.ITaskEvent;

import java.io.Closeable;

public abstract interface Channel
  extends Closeable, Monitorable
{
  public abstract void publish(ITaskEvent paramITaskEvent)
    throws Exception;
  
  public abstract void addSubscriber(Link paramLink)
    throws Exception;
  
  public abstract void removeSubscriber(Link paramLink);
  
  public abstract void addCallback(NewSubscriberAddedCallback paramNewSubscriberAddedCallback);
  
  public abstract int getSubscribersCount();
  
  public static abstract interface NewSubscriberAddedCallback
  {
    public abstract void notifyMe(Link paramLink);
  }
}

