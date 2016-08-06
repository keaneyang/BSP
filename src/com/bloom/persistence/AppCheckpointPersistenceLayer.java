package com.bloom.persistence;

import com.bloom.intf.PersistenceLayer;
import com.bloom.intf.PersistenceLayer.Range;
import com.bloom.recovery.AppCheckpoint;
import com.bloom.waction.Waction;
import com.bloom.waction.WactionKey;
import com.bloom.recovery.Path.ItemList;
import com.bloom.recovery.Position;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;
import javax.persistence.Query;
import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;

public class AppCheckpointPersistenceLayer
  implements PersistenceLayer
{
  private static Logger logger = Logger.getLogger(AppCheckpointPersistenceLayer.class);
  private EntityManagerFactory factory;
  private Map<String, Object> props = new HashMap();
  private final String persistenceUnitName;
  
  public AppCheckpointPersistenceLayer(String persistenceUnit, Map<String, Object> properties)
  {
    this.persistenceUnitName = persistenceUnit;
    if (properties != null)
    {
      this.props = new HashMap();
      this.props.putAll(properties);
    }
  }
  
  public void init()
  {
    if ((this.persistenceUnitName == null) || (this.persistenceUnitName.isEmpty()))
    {
      logger.warn("provide a unique name for persistence unit and properties before calling init method");
      return;
    }
    synchronized (this)
    {
      if ((this.factory == null) || (!this.factory.isOpen())) {
        if ((this.props == null) || (this.props.isEmpty())) {
          this.factory = Persistence.createEntityManagerFactory(this.persistenceUnitName);
        } else {
          this.factory = Persistence.createEntityManagerFactory(this.persistenceUnitName, this.props);
        }
      }
    }
  }
  
  public void init(String name)
  {
    init();
  }
  
  public int delete(Object object)
  {
    int noofdeletes = 0;
    synchronized (this.factory)
    {
      EntityTransaction txn = null;
      EntityManager entityManager = null;
      try
      {
        int i;
        if (object == null)
        {
          logger.warn("Got null object to delete so, simply skip everything from here");
          i = -1;
          if ((txn != null) && (txn.isActive())) {
            txn.rollback();
          }
          if (entityManager != null)
          {
            entityManager.clear();
            entityManager.close();
          }
          return i;
        }
        if (!(object instanceof List))
        {
          logger.warn("AppCheckpointPersistenceLayer.delete() expects a List<AppCheckpoint> but received " + object.getClass().getCanonicalName());
          i = -1;
          if ((txn != null) && (txn.isActive())) {
            txn.rollback();
          }
          if (entityManager != null)
          {
            entityManager.clear();
            entityManager.close();
          }
          return i;
        }
        Object untypedList = (List)object;
        List<AppCheckpoint> appCheckpoints = new ArrayList();
        Object item;
        for (Iterator i$ = ((List)untypedList).iterator(); i$.hasNext(); appCheckpoints.add((AppCheckpoint)item))
        {
          item = i$.next();
          if (!(item instanceof AppCheckpoint))
          {
            logger.warn("AppCheckpointPersistenceLayer.delete() received a list with items other than AppCheckpoints");
            int j = -1;
            if ((txn != null) && (txn.isActive())) {
              txn.rollback();
            }
            if (entityManager != null)
            {
              entityManager.clear();
              entityManager.close();
            }
            return j;
          }
        }
        entityManager = getEntityManager();
        txn = entityManager.getTransaction();
        txn.begin();
        String query = "DELETE FROM AppCheckpoint c WHERE c.flowUuid=:flowUuid AND c.pathItems=:pathItems";
        for (AppCheckpoint appCheckpoint : appCheckpoints)
        {
          Map<String, Object> params = new HashMap();
          
          Query q = entityManager.createQuery(query);
          q.setParameter("flowUuid", appCheckpoint.flowUuid);
          q.setParameter("pathItems", appCheckpoint.pathItems.toString());
          q.executeUpdate();
          noofdeletes++;
        }
        txn.commit();
        if (logger.isTraceEnabled()) {
          logger.trace("AppCheckpoint deleted from disk: " + noofdeletes);
        }
      }
      finally
      {
        if ((txn != null) && (txn.isActive())) {
          txn.rollback();
        }
        if (entityManager != null)
        {
          entityManager.clear();
          entityManager.close();
        }
      }
    }
    return noofdeletes;
  }
  
  public PersistenceLayer.Range[] persist(Object object)
  {
    if (Logger.getLogger("Recovery").isTraceEnabled()) {
      Logger.getLogger("Recovery").trace("AppCheckpointPersistenceLayer.persist(" + object + ")");
    }
    int noofinserts = 0;
    PersistenceLayer.Range[] rangeArray = null;
    synchronized (this.factory)
    {
      EntityTransaction txn = null;
      EntityManager entityManager = null;
      try
      {
        PersistenceLayer.Range[] arrayOfRange1;
        if (object == null)
        {
          Logger.getLogger("Recovery").warn("Persist object is null, aborting");
          arrayOfRange1 = rangeArray;
          if ((txn != null) && (txn.isActive())) {
            txn.rollback();
          }
          if (entityManager != null)
          {
            entityManager.clear();
            entityManager.close();
          }
          return arrayOfRange1;
        }
        if (!(object instanceof List))
        {
          Logger.getLogger("Recovery").warn("AppCheckpointPersistenceLayer.persist() expects a List<AppCheckpoint> but received " + object.getClass().getCanonicalName());
          arrayOfRange1 = rangeArray;
          if ((txn != null) && (txn.isActive())) {
            txn.rollback();
          }
          if (entityManager != null)
          {
            entityManager.clear();
            entityManager.close();
          }
          return arrayOfRange1;
        }
        Object untypedList = (List)object;
        List<AppCheckpoint> appCheckpoints = new ArrayList();
        Object item;
        for (Iterator i$ = ((List)untypedList).iterator(); i$.hasNext(); appCheckpoints.add((AppCheckpoint)item))
        {
          item = i$.next();
          if (!(item instanceof AppCheckpoint))
          {
            Logger.getLogger("Recovery").warn("AppCheckpointPersistenceLayer.persist() received a list with items other than AppCheckpoints");
            PersistenceLayer.Range[] arrayOfRange2 = rangeArray;
            if ((txn != null) && (txn.isActive())) {
              txn.rollback();
            }
            if (entityManager != null)
            {
              entityManager.clear();
              entityManager.close();
            }
            return arrayOfRange2;
          }
        }
        entityManager = getEntityManager();
        txn = entityManager.getTransaction();
        txn.begin();
        String query = "SELECT c FROM AppCheckpoint c WHERE c.flowUuid=:flowUuid AND c.pathItems=:pathItems";
        if (Logger.getLogger("Recovery").isDebugEnabled()) {
          Logger.getLogger("Recovery").debug("STARTING INSERTS: " + noofinserts);
        }
        for (AppCheckpoint appCheckpoint : appCheckpoints)
        {
          Map<String, Object> params = new HashMap();
          params.put("flowUuid", appCheckpoint.flowUuid);
          params.put("pathItems", appCheckpoint.pathItems.toString());
          List<?> result = runQuery(query, params, Integer.valueOf(1));
          if ((result == null) || (result.size() == 0))
          {
            entityManager.persist(appCheckpoint);
            noofinserts++;
          }
          else
          {
            appCheckpoint.id = ((AppCheckpoint)result.get(0)).id;
            entityManager.merge(appCheckpoint);
            noofinserts++;
          }
        }
        if (Logger.getLogger("Recovery").isDebugEnabled()) {
          Logger.getLogger("Recovery").debug("INSERTS DONE: " + noofinserts);
        }
        txn.commit();
        if (Logger.getLogger("Recovery").isTraceEnabled()) {
          Logger.getLogger("Recovery").trace("AppCheckpoint written to disk: " + noofinserts);
        }
      }
      finally
      {
        if ((txn != null) && (txn.isActive())) {
          txn.rollback();
        }
        if (entityManager != null)
        {
          entityManager.clear();
          entityManager.close();
        }
      }
    }
    rangeArray = new PersistenceLayer.Range[1];
    rangeArray[0] = new PersistenceLayer.Range(0, noofinserts);
    return rangeArray;
  }
  
  public void merge(Object object)
  {
    if (object == null) {
      throw new IllegalArgumentException("Cannot persist null object");
    }
    EntityManager entityManager = null;
    EntityTransaction txn = null;
    try
    {
      entityManager = getEntityManager();
      txn = entityManager.getTransaction();
      txn.begin();
      entityManager.merge(object);
      txn.commit();
    }
    finally
    {
      if ((txn != null) && (txn.isActive())) {
        txn.rollback();
      }
      if (entityManager != null)
      {
        entityManager.clear();
        entityManager.close();
      }
    }
  }
  
  public Object get(Class<?> objectClass, Object objectId)
  {
    Object result = null;
    EntityManager entityManager = null;
    try
    {
      entityManager = getEntityManager();
      result = entityManager.find(objectClass, objectId);
    }
    finally
    {
      entityManager.clear();
      entityManager.close();
    }
    return result;
  }
  
  public void setStoreName(String storeName, String tableName)
  {
    logger.warn("Cannot change store name of AppChecpointPersistenceLayer from " + this.persistenceUnitName + " to " + storeName);
  }
  
  public int executeUpdate(String queryString, Map<String, Object> params)
  {
    EntityManager em = getEntityManager();
    EntityTransaction txn = null;
    synchronized (this.factory)
    {
      try
      {
        Query query = em.createQuery(queryString);
        if (params != null) {
          for (String key : params.keySet()) {
            query.setParameter(key, params.get(key));
          }
        }
        txn = em.getTransaction();
        txn.begin();
        int queryResults = query.executeUpdate();
        txn.commit();
        
        return queryResults;
      }
      catch (Throwable t)
      {
        logger.error(t.getLocalizedMessage());
        if ((txn != null) && (txn.isActive())) {
          txn.rollback();
        }
      }
    }
    return 0;
  }
  
  public Object runNativeQuery(String query)
  {
    EntityManager entityManager = null;
    Object result = null;
    try
    {
      entityManager = getEntityManager();
      Query q = entityManager.createNativeQuery(query);
      result = q.getSingleResult();
    }
    catch (Exception ex)
    {
      ex.printStackTrace();
      logger.error("AppCheckpointPersistenceLayer error while executing native query: " + query);
    }
    finally
    {
      entityManager.clear();
      entityManager.close();
    }
    return result;
  }
  
  public List<?> runQuery(String query, Map<String, Object> params, Integer maxResults)
  {
    List<?> queryResults = null;
    EntityManager entityManager = null;
    try
    {
      entityManager = getEntityManager();
      Query q = entityManager.createQuery(query);
      if ((maxResults != null) && (maxResults.intValue() > 0)) {
        q.setMaxResults(maxResults.intValue());
      }
      if (params != null) {
        for (String key : params.keySet()) {
          q.setParameter(key, params.get(key));
        }
      }
      queryResults = q.getResultList();
    }
    catch (IllegalArgumentException e)
    {
      logger.error(e.getMessage(), e);
    }
    finally
    {
      if (entityManager != null)
      {
        entityManager.clear();
        entityManager.close();
      }
    }
    return queryResults;
  }
  
  public void close()
  {
    if (this.factory != null)
    {
      if (this.factory.isOpen()) {
        this.factory.close();
      }
      this.factory = null;
    }
  }
  
  public Position getWSPosition(String namespaceName, String wactionStoreName)
  {
    throw new NotImplementedException("This method is not supported for AppCheckpointPersistenceLayer");
  }
  
  public boolean clearWSPosition(String namespaceName, String wactionStoreName)
  {
    throw new NotImplementedException("This method is not supported for AppCheckpointPersistenceLayer");
  }
  
  protected synchronized EntityManager getEntityManager()
  {
    EntityManager entityManager = null;
    if ((this.props == null) || (this.props.isEmpty())) {
      entityManager = this.factory.createEntityManager();
    } else {
      entityManager = this.factory.createEntityManager(this.props);
    }
    return entityManager;
  }
  
  public <T extends Waction> Iterable<T> getResults(Class<T> objectClass, Map<String, Object> filter, Set<WactionKey> excludeKeys)
  {
    return null;
  }
  
  public <T extends Waction> Iterable<T> getResults(Class<T> objectClass, String wactionKey, Map<String, Object> filter)
  {
    return null;
  }
  
  public WactionStore getWactionStore()
  {
    return null;
  }
  
  public void setWactionStore(WactionStore ws) {}
}
