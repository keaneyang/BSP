package com.bloom.persistence;

import com.bloom.intf.PersistenceLayer;
import com.bloom.intf.PersistencePolicy;
import com.bloom.intf.PersistenceLayer.Range;
import com.bloom.runtime.BaseServer;
import com.bloom.runtime.CustomThreadFactory;
import com.bloom.runtime.components.EntityType;
import com.bloom.waction.Waction;
import com.bloom.wactionstore.Utility;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

public class PeriodicPersistencePolicy
  implements PersistencePolicy
{
  private static Logger logger = Logger.getLogger(PeriodicPersistencePolicy.class);
  private static final int DEFAULT_MAX_PERSIST_QUEUE_SIZE = 100000;
  private final PersistenceLayer persistenceLayer;
  protected long periodMillis;
  final ScheduledThreadPoolExecutor executor;
  ScheduledFuture<?> scheduledFlusher = null;
  private final BlockingQueue<Waction> persistQueue;
  private final WactionStore ws;
  private Thread emergencyFlushThread = null;
  private Object emergencyFlushThreadFlag = new Object();
  private int batchWritingSize;
  
 
  
  public PeriodicPersistencePolicy(long milliseconds, PersistenceLayer persistenceLayer, BaseServer srv, Map<String, Object> props, WactionStore ws)
  {
    this.periodMillis = milliseconds;
    this.persistenceLayer = persistenceLayer;
    this.ws = ws;
    this.persistenceLayer.setWactionStore(ws);
    this.executor = new ScheduledThreadPoolExecutor(1, new CustomThreadFactory("PeriodicPersistence_Scheduler"));
    this.executor.setRemoveOnCancelPolicy(true);
    
    Runnable flushRunnable = new Runnable()
    {
      public void run()
      {
        PeriodicPersistencePolicy.this.flush();
      }
    };
    this.scheduledFlusher = this.executor.scheduleWithFixedDelay(flushRunnable, this.periodMillis, this.periodMillis, TimeUnit.MILLISECONDS);
    try
    {
      this.batchWritingSize = Utility.extractInt(props.get("BATCH_WRITING_SIZE"));
    }
    catch (Exception e)
    {
      this.batchWritingSize = 10000;
    }
    int capacity;
    try
    {
      capacity = Utility.extractInt(props.get("MAX_PERSIST_QUEUE_SIZE"));
    }
    catch (Exception e1)
    {
      try
      {
        capacity = 2 * Utility.extractInt(props.get("MAX_THREAD_NUM")) * this.batchWritingSize;
      }
      catch (Exception e2)
      {
        try
        {
          capacity = Integer.parseInt(System.getProperty("com.bloom.optimalBackPressureThreshold", "10000"));
        }
        catch (Exception e)
        {
          logger.warn("Invalid setting for com.bloom.optimalBackPressureThreshold, defaulting to 10000");
          capacity = 10000;
        }
      }
    }
    this.persistQueue = new LinkedBlockingQueue(capacity);
  }
  
  public void close()
  {
    flush();
    this.scheduledFlusher.cancel(true);
    shutdownExecutor();
  }
  
  public void shutdownExecutor()
  {
    this.executor.shutdown();
    try
    {
      if (!this.executor.awaitTermination(5L, TimeUnit.SECONDS))
      {
        this.executor.shutdownNow();
        if (!this.executor.awaitTermination(60L, TimeUnit.SECONDS)) {
          System.err.println("PeriodicPersistence_Scheduler did not terminate");
        }
      }
    }
    catch (InterruptedException ie)
    {
      this.executor.shutdown();
      Thread.currentThread().interrupt();
    }
  }
  
  public synchronized void flush()
  {
    do
    {
      if (this.ws.isCrashed)
      {
        if (logger.isInfoEnabled()) {
          logger.info("Waction store :" + this.ws.getMetaFullName() + ", is flagged as crashed. Notifying app manager.");
        }
        this.ws.notifyAppMgr(EntityType.WACTIONSTORE, this.ws.getMetaName(), this.ws.getMetaID(), this.ws.crashCausingException, "Persist waction", new Object[0]);
      }
      long TIME1 = 0L;
      long TIME2 = 0L;
      if (this.persistQueue.isEmpty()) {
        return;
      }
      List<Waction> writeWactions = new ArrayList();
      synchronized (this.persistQueue)
      {
        writeWactions.addAll(this.persistQueue);
      }
      if (logger.isDebugEnabled()) {
        TIME1 = System.currentTimeMillis();
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Number of wactions to be persisted: " + writeWactions.size());
      }
      PersistenceLayer.Range[] successRanges = this.persistenceLayer.persist(writeWactions);
      if (logger.isDebugEnabled()) {
        TIME2 = System.currentTimeMillis();
      }
      if (logger.isDebugEnabled()) {
        logger.debug("WactionStore Periodic Persistence : Total time taken to persist " + writeWactions.size() + " wactions is :" + (TIME2 - TIME1) + " milli secs. Rate : " + Math.floor(writeWactions.size() * 1000 / (TIME2 - TIME1)));
      }
      if ((successRanges != null) && (successRanges.length > 0)) {
        synchronized (this.persistQueue)
        {
          int index = 0;
          Iterator<Waction> iter = this.persistQueue.iterator();
          if (iter.hasNext()) {
            for (int ij = 0; ij < successRanges.length; ij++)
            {
              int startIndex = successRanges[ij].getStart();
              int endIndex = successRanges[ij].getEnd();
              if ((startIndex != 0) || (endIndex != 0))
              {
                if (successRanges[ij].isSuccessful())
                {
                  if (logger.isDebugEnabled()) {
                    logger.debug("removing wactions : index=" + index + ", startIndex=" + startIndex + ", endIndex=" + endIndex);
                  }
                  while ((index >= startIndex) && (index < endIndex))
                  {
                    iter.next();
                    iter.remove();
                    index++;
                  }
                }
                if (logger.isDebugEnabled()) {
                  logger.debug("skipping wactions : index=" + index + ", startIndex=" + startIndex + ", endIndex=" + endIndex);
                }
                while ((index >= startIndex) && (index < endIndex))
                {
                  iter.next();
                  index++;
                }
              }
            }
          }
        }
      } else if (logger.isDebugEnabled()) {
        logger.debug("Empty PersistenceLayer.Range  returned from persist method.");
      }
    } while (this.persistQueue.size() >= this.batchWritingSize);
  }
  
  public boolean addWaction(Waction w)
  {
    try
    {
      if ((this.emergencyFlushThread == null) && (this.persistQueue.remainingCapacity() <= this.persistQueue.size() / 19)) {
        synchronized (this.emergencyFlushThreadFlag)
        {
          if (this.emergencyFlushThread == null)
          {
            Runnable emergencyFlushRunnable = new Runnable()
            {
              public void run()
              {
                try
                {
                  PeriodicPersistencePolicy.this.flush();
                }
                finally
                {
                  PeriodicPersistencePolicy.this.emergencyFlushThread = null;
                }
              }
            };
            this.emergencyFlushThread = new Thread(emergencyFlushRunnable);
            this.emergencyFlushThread.start();
          }
        }
      }
      this.persistQueue.put(w);
      return true;
    }
    catch (InterruptedException e)
    {
      logger.error("Unable to add waction to persist queue", e);
    }
    return false;
  }
  
  public Set<Waction> getUnpersistedWactions()
  {
    return new HashSet(this.persistQueue);
  }
}
