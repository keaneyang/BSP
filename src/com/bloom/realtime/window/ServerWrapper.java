package com.bloom.runtime.window;

import java.util.concurrent.ScheduledExecutorService;

import com.bloom.runtime.Server;
import com.bloom.runtime.channels.Channel;
import com.bloom.runtime.components.FlowComponent;
import com.bloom.runtime.components.Publisher;
import com.bloom.runtime.components.Subscriber;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Window;

abstract interface ServerWrapper
{
  public abstract Channel createChannel(FlowComponent paramFlowComponent);
  
  public abstract void destroyKeyFactory(MetaInfo.Window paramWindow)
    throws Exception;
  
  public abstract void subscribe(Publisher paramPublisher, Subscriber paramSubscriber)
    throws Exception;
  
  public abstract void unsubscribe(Publisher paramPublisher, Subscriber paramSubscriber)
    throws Exception;
  
  public abstract ScheduledExecutorService getScheduler();
  
  public abstract Server getServer();
}

