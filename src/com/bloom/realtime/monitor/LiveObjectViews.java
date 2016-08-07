package com.bloom.runtime.monitor;

import java.util.Collection;

import com.bloom.runtime.channels.Channel;
import com.bloom.runtime.components.FlowComponent;

public abstract interface LiveObjectViews
{
  public abstract Collection<FlowComponent> getAllObjectsView();
  
  public abstract Collection<Channel> getAllChannelsView();
}

