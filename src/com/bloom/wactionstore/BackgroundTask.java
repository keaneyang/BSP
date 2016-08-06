package com.bloom.wactionstore;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.log4j.Logger;

import com.bloom.persistence.WactionStore;

public abstract class BackgroundTask
  implements Runnable
{
  private static final Class<BackgroundTask> thisClass = BackgroundTask.class;
  private static final Logger logger = Logger.getLogger(thisClass);
  private final Lock backgroundThreadLock = new ReentrantLock();
  private final Condition backgroundThreadSignal = this.backgroundThreadLock.newCondition();
  private final AtomicBoolean terminationRequest = new AtomicBoolean(false);
  private final String wActionStoreName;
  private final WactionStore ws;
  private final String threadName;
  private final long timeoutMilliseconds;
  private Thread backgroundThread = null;
  
  protected BackgroundTask(String wActionStoreName, String dataTypeName, long timeoutMilliseconds, WactionStore ws)
  {
    this.wActionStoreName = wActionStoreName;
    this.threadName = String.format("BackgroundTask-%s-%s", new Object[] { wActionStoreName, dataTypeName });
    this.timeoutMilliseconds = timeoutMilliseconds;
    this.ws = ws;
  }
  
  public WactionStore getWactionStoreObject()
  {
    return this.ws;
  }
  
  public void waitForSignal()
  {
    this.backgroundThreadLock.lock();
    try
    {
      this.backgroundThreadSignal.await(this.timeoutMilliseconds, TimeUnit.MILLISECONDS);
    }
    catch (InterruptedException ignored) {}finally
    {
      this.backgroundThreadLock.unlock();
    }
  }
  
  public void signalBackgroundThread()
  {
    this.backgroundThreadLock.lock();
    try
    {
      this.backgroundThreadSignal.signalAll();
    }
    finally
    {
      this.backgroundThreadLock.unlock();
    }
  }
  
  public void start()
  {
    synchronized (this.backgroundThreadLock)
    {
      if (!isAlive())
      {
        this.terminationRequest.set(false);
        this.backgroundThread = new Thread(this, this.threadName);
        WActionStores.registerBackgroundTask(this);
        this.backgroundThread.start();
        logger.info(String.format("Started background task '%s' for WActionStore '%s'", new Object[] { this.threadName, this.wActionStoreName }));
      }
    }
  }
  
  public void terminate()
  {
    synchronized (this.backgroundThreadLock)
    {
      WActionStores.unregisterBackgroundTask(this);
      this.terminationRequest.set(true);
      if (isAlive())
      {
        logger.info(String.format("Terminating background task '%s' for WActionStore '%s'", new Object[] { this.threadName, this.wActionStoreName }));
        signalBackgroundThread();
        try
        {
          this.backgroundThread.join(this.timeoutMilliseconds);
          if (isAlive()) {
            logger.warn("Failed to terminate the background thread " + String.format("Terminating background task '%s' for WActionStore '%s'", new Object[] { this.threadName, this.wActionStoreName }));
          }
        }
        catch (InterruptedException ignored) {}
      }
      else
      {
        logger.info(String.format("Background task '%s' for WActionStore '%s' already terminated", new Object[] { this.threadName, this.wActionStoreName }));
      }
    }
  }
  
  public boolean isTerminationRequested()
  {
    return this.terminationRequest.get();
  }
  
  public boolean isAlive()
  {
    synchronized (this.backgroundThreadLock)
    {
      return (this.backgroundThread != null) && (this.backgroundThread.isAlive());
    }
  }
  
  public String getWActionStoreName()
  {
    return this.wActionStoreName;
  }
}
