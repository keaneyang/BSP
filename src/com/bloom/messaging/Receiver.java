package com.bloom.messaging;

import java.util.Map;

public abstract interface Receiver
{
  public abstract void start(Map<Object, Object> paramMap)
    throws Exception;
  
  public abstract boolean stop()
    throws Exception;
}

