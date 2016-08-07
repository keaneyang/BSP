package com.bloom.runtime.window;

class NoTimerPolicy
  implements WTimerPolicy
{
  public void startTimer(ScalaWindow w) {}
  
  public void stopTimer(ScalaWindow w) {}
  
  public void onTimer(ScalaWindow w)
    throws Exception
  {}
  
  public void updateWakeupQueue(ScalaWindow w, WBuffer b, long now) {}
}

