package com.bloom.runtime;

public enum ActionType
{
  CREATE,  START,  VERIFY_START,  START_SOURCES,  STOP,  CRASH,  DEPLOY,  INIT_CACHES,  START_CACHES,  STOP_CACHES,  UNDEPLOY,  RESUME,  STATUS,  START_ADHOC,  STOP_ADHOC,  CONTINUE,  IGNORE,  RETRY,  SHUTDOWN,  LOAD,  UNLOAD,  BOOT,  RESTART;
  
  private ActionType() {}
  
  public static boolean contains(String val)
  {
    for (ActionType et : ) {
      if (et.name().equalsIgnoreCase(val)) {
        return true;
      }
    }
    return false;
  }
}
