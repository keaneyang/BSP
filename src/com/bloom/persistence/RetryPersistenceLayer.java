package com.bloom.persistence;

import com.bloom.intf.PersistenceLayer;
import com.bloom.intf.PersistenceLayer.Range;
import com.bloom.waction.Waction;
import com.bloom.waction.WactionKey;
import com.bloom.recovery.Position;

import java.sql.SQLNonTransientConnectionException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.persistence.PersistenceException;
import javax.persistence.RollbackException;
import org.apache.log4j.Logger;
import org.eclipse.persistence.exceptions.DatabaseException;

public class RetryPersistenceLayer
  implements PersistenceLayer
{
  private final PersistenceLayer persistenceLayer;
  private static Logger logger = Logger.getLogger(RetryPersistenceLayer.class);
  private final int retryCount;
  private final int retryInterval;
  
  public RetryPersistenceLayer(PersistenceLayer persistenceLayer)
  {
    this.persistenceLayer = persistenceLayer;
    this.retryCount = Integer.valueOf(System.getProperty("com.bloom.config.persist.maxRetries", "0")).intValue();
    this.retryInterval = Integer.valueOf(System.getProperty("com.bloom.config.persist.retryInterval", "10")).intValue();
  }
  
  public void init()
  {
    this.persistenceLayer.init();
  }
  
  public void init(String storeName)
  {
    this.persistenceLayer.init(storeName);
  }
  
  public int delete(Object object)
  {
    return this.persistenceLayer.delete(object);
  }
  
  public PersistenceLayer.Range[] persist(Object object)
  {
    int retryAttempt = 0;
    for (;;)
    {
      try
      {
        return this.persistenceLayer.persist(object);
      }
      catch (Exception e)
      {
        logger.error("Unable to write status data to table because of a communication failure: " + e.getMessage());
        if ((e instanceof DatabaseException))
        {
          if ((((DatabaseException)e).getDatabaseErrorCode() == 40000) && ((((DatabaseException)e).getInternalException() instanceof SQLNonTransientConnectionException))) {
            if (retryAttempt < this.retryCount)
            {
              logger.warn("Retry attempt... " + retryAttempt + " ..waiting before next attempt......." + this.retryInterval + " seconds.");
              try
              {
                Thread.sleep(this.retryInterval * 1000);
              }
              catch (InterruptedException e1)
              {
                throw e;
              }
              retryAttempt++;
            }
            else
            {
              logger.error("Shutting down server becasue it could not connect to dataabase with error  " + e.getMessage());
              System.exit(1);
            }
          }
        }
        else if ((e instanceof PersistenceException))
        {
          if (retryAttempt < this.retryCount)
          {
            logger.warn("Retry attempt... " + retryAttempt + " ..waiting before next attempt......." + this.retryInterval + " seconds.");
            try
            {
              Thread.sleep(this.retryInterval * 1000);
            }
            catch (InterruptedException e1)
            {
              throw e;
            }
            retryAttempt++;
          }
          else
          {
            logger.error("Shutting down server becasue it could not connect to dataabase with error " + e.getMessage());
            System.exit(1);
          }
        }
        else
        {
          if ((e instanceof RollbackException))
          {
            logger.error("Rolling back attempt to write status data to table: " + e.getMessage());
            throw e;
          }
          logger.error("Unable to write status data to table: " + e.getMessage());
          throw e;
        }
      }
    }
  }
  
  public void merge(Object object)
  {
    this.persistenceLayer.merge(object);
  }
  
  public void setStoreName(String storeName, String tableName)
  {
    this.persistenceLayer.setStoreName(storeName, tableName);
  }
  
  public Object get(Class<?> objectClass, Object objectId)
  {
    return this.persistenceLayer.get(objectClass, objectId);
  }
  
  public List<?> runQuery(String query, Map<String, Object> params, Integer maxResults)
  {
    int retryAttempt = 0;
    for (;;)
    {
      try
      {
        return this.persistenceLayer.runQuery(query, params, maxResults);
      }
      catch (Exception e)
      {
        logger.error("Unable to write status data to table because of a communication failure: " + e.getMessage());
        if ((e instanceof DatabaseException))
        {
          if ((((DatabaseException)e).getDatabaseErrorCode() == 40000) && ((((DatabaseException)e).getInternalException() instanceof SQLNonTransientConnectionException))) {
            if (retryAttempt < this.retryCount)
            {
              logger.warn("Retry attempt... " + retryAttempt + " ..waiting before next attempt......." + this.retryInterval + " seconds.");
              try
              {
                Thread.sleep(this.retryInterval * 1000);
              }
              catch (InterruptedException e1)
              {
                throw e;
              }
              retryAttempt++;
            }
            else
            {
              logger.error("Shutting down server becasue it could not connect to dataabase with error  " + e.getMessage());
              System.exit(1);
            }
          }
        }
        else if ((e instanceof PersistenceException))
        {
          if (retryAttempt < this.retryCount)
          {
            logger.warn("Retry attempt... " + retryAttempt + " ..waiting before next attempt......." + this.retryInterval + " seconds.");
            try
            {
              Thread.sleep(this.retryInterval * 1000);
            }
            catch (InterruptedException e1)
            {
              throw e;
            }
            retryAttempt++;
          }
          else
          {
            logger.error("Shutting down server becasue it could not connect to dataabase with error " + e.getMessage());
            System.exit(1);
          }
        }
        else
        {
          if ((e instanceof RollbackException))
          {
            logger.error("Rolling back attempt to write status data to table: " + e.getMessage());
            throw e;
          }
          logger.error("Unable to write status data to table: " + e.getMessage());
          throw e;
        }
      }
    }
  }
  
  public Object runNativeQuery(String query)
  {
    return this.persistenceLayer.runNativeQuery(query);
  }
  
  public int executeUpdate(String query, Map<String, Object> params)
  {
    return this.persistenceLayer.executeUpdate(query, params);
  }
  
  public void close()
  {
    this.persistenceLayer.close();
  }
  
  public Position getWSPosition(String namespaceName, String wactionStoreName)
  {
    return this.persistenceLayer.getWSPosition(namespaceName, wactionStoreName);
  }
  
  public boolean clearWSPosition(String namespaceName, String wactionStoreName)
  {
    return this.persistenceLayer.clearWSPosition(namespaceName, wactionStoreName);
  }
  
  public <T extends Waction> Iterable<T> getResults(Class<T> objectClass, Map<String, Object> filter, Set<WactionKey> excludeKeys)
  {
    return this.persistenceLayer.getResults(objectClass, filter, excludeKeys);
  }
  
  public <T extends Waction> Iterable<T> getResults(Class<T> objectClass, String wactionKey, Map<String, Object> filter)
  {
    return this.persistenceLayer.getResults(objectClass, wactionKey, filter);
  }
  
  public WactionStore getWactionStore()
  {
    return this.persistenceLayer.getWactionStore();
  }
  
  public void setWactionStore(WactionStore ws)
  {
    this.persistenceLayer.setWactionStore(ws);
  }
}
