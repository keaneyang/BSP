package com.bloom.runtime.window;

abstract interface WTimerPolicy
{
  public abstract void startTimer(ScalaWindow paramScalaWindow);
  
  public abstract void stopTimer(ScalaWindow paramScalaWindow);
  
  public abstract void onTimer(ScalaWindow paramScalaWindow)
    throws Exception;
  
  public abstract void updateWakeupQueue(ScalaWindow paramScalaWindow, WBuffer paramWBuffer, long paramLong);
}

