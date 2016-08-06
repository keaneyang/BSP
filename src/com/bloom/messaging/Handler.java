package com.bloom.messaging;

import com.bloom.runtime.components.FlowComponent;

public abstract interface Handler
{
  public abstract void onMessage(Object paramObject);
  
  public abstract String getName();
  
  public abstract FlowComponent getOwner();
}
