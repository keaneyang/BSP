package com.bloom.runtime.window;

import java.util.concurrent.Future;

abstract class BaseTimerPolicy
  implements WTimerPolicy
{
  Future<?> task;
  
  public void stopTimer(ScalaWindow w)
  {
    if (this.task != null)
    {
      this.task.cancel(true);
      this.task = null;
    }
  }
}
