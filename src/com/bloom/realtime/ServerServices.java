package com.bloom.runtime;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

public abstract interface ServerServices
{
  public abstract ScheduledExecutorService getScheduler();
  
  public abstract ExecutorService getThreadPool();
  
  public abstract ScheduledFuture<?> scheduleStatsReporting(Runnable paramRunnable, int paramInt, boolean paramBoolean);
}

