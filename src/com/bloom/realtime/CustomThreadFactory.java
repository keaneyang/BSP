package com.bloom.runtime;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class CustomThreadFactory
  implements ThreadFactory
{
  private final AtomicInteger threadNumber = new AtomicInteger();
  private final String threadPoolName;
  private final boolean daemons;
  
  public CustomThreadFactory(String threadPoolName)
  {
    this(threadPoolName, false);
  }
  
  public CustomThreadFactory(String threadPoolName, boolean daemons)
  {
    this.threadPoolName = threadPoolName;
    this.daemons = daemons;
  }
  
  public Thread newThread(Runnable r)
  {
    Thread t = new Thread(r, this.threadPoolName + "-" + this.threadNumber.incrementAndGet());
    
    t.setDaemon(this.daemons);
    return t;
  }
}
