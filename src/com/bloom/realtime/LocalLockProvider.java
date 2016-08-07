package com.bloom.runtime;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.log4j.Logger;

public class LocalLockProvider
{
  private static Logger logger = Logger.getLogger(LocalLockProvider.class);
  private static final Map<String, Lock> lockMap = new HashMap();
  private static Lock lockForLockMap = new ReentrantLock();
  
  public static Lock getLock(Key key)
  {
    lockForLockMap.lock();
    try
    {
      if (lockMap.get(key.toString()) == null)
      {
        lockMap.put(key.toString(), new CustomLock());
        if (logger.isDebugEnabled()) {
          logger.debug("created lock for key : " + key);
        }
      }
    }
    finally
    {
      lockForLockMap.unlock();
    }
    return (Lock)lockMap.get(key.toString());
  }
  
  public static void removeLock(Key key)
  {
    lockForLockMap.lock();
    try
    {
      if (lockMap.get(key.toString()) != null)
      {
        lockMap.remove(key.toString());
        if (logger.isDebugEnabled()) {
          logger.debug("removed lock for key : " + key);
        }
      }
    }
    finally
    {
      lockForLockMap.unlock();
    }
  }
  
  private static class CustomLock
    extends ReentrantLock
  {
    private static final long serialVersionUID = -1486927071175970257L;
    
    public void lock()
    {
      super.lock();
    }
    
    public void unlock()
    {
      super.unlock();
    }
  }
  
  public static class Key
  {
    String part1;
    String part2;
    
    public Key(String part1, String part2)
    {
      this.part1 = part1;
      this.part2 = part2;
    }
    
    public String get()
    {
      return this.part1 + "-" + this.part2;
    }
    
    public String toString()
    {
      return get();
    }
  }
}
