package com.bloom.runtime.window;

import java.util.LinkedList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

class SlidingTimerPolicy
  extends BaseTimerPolicy
{
  public void startTimer(ScalaWindow w) {}
  
  static class TimerTask
  {
    final WBuffer buffer;
    final long createdTimestamp;
    
    TimerTask(WBuffer b, long now)
    {
      this.buffer = b;
      this.createdTimestamp = now;
    }
  }
  
  private boolean firstTime = true;
  private LinkedList<TimerTask> timeTaskQueue = new LinkedList();
  
  public void onTimer(ScalaWindow w)
    throws Exception
  {
    long now = System.nanoTime();
    long waitTime = w.interval;
    while (!this.timeTaskQueue.isEmpty())
    {
      TimerTask t = (TimerTask)this.timeTaskQueue.peek();
      if (t.createdTimestamp + w.interval > now)
      {
        waitTime = t.createdTimestamp + w.interval - now;
        break;
      }
      t = (TimerTask)this.timeTaskQueue.remove();
      
      t.buffer.removeTill(now);
    }
    this.task = w.getScheduler().schedule(w, waitTime, TimeUnit.NANOSECONDS);
  }
  
  public void updateWakeupQueue(ScalaWindow w, WBuffer b, long now)
  {
    this.timeTaskQueue.add(new TimerTask(b, now));
    if (this.firstTime)
    {
      this.task = w.getScheduler().schedule(w, w.interval, TimeUnit.NANOSECONDS);
      this.firstTime = false;
    }
  }
}
