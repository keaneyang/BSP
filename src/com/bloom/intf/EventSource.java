package com.bloom.intf;

public abstract interface EventSource
{
  public abstract void addEventSink(EventSink paramEventSink);
  
  public abstract void removeEventSink(EventSink paramEventSink);
}

