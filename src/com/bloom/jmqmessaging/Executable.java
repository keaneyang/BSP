package com.bloom.jmqmessaging;

import java.util.concurrent.Callable;

public abstract class Executable<O, V>
  implements Callable<V>
{
  private O on;
  
  public void setOn(O on)
  {
    this.on = on;
  }
  
  public O on()
  {
    return (O)this.on;
  }
  
  public abstract V call();
  
  public abstract boolean hasResponse();
}
