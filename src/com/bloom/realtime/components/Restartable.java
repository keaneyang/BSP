package com.bloom.runtime.components;

public abstract interface Restartable
{
  public abstract void start()
    throws Exception;
  
  public abstract void stop()
    throws Exception;
  
  public abstract boolean isRunning();
}

