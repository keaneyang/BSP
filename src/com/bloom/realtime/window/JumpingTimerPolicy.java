package com.bloom.runtime.window;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

class JumpingTimerPolicy
  extends BaseTimerPolicy
{
  public void startTimer(ScalaWindow w)
  {
    this.task = w.getScheduler().scheduleAtFixedRate(w, w.interval, w.interval, TimeUnit.NANOSECONDS);
  }
  
  public void onTimer(ScalaWindow w)
    throws Exception
  {
    w.jumpingWindowOnTimerCallback();
  }
  
  public void updateWakeupQueue(ScalaWindow w, WBuffer b, long now) {}
}
