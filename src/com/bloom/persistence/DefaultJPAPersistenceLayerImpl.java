package com.bloom.persistence;

import com.bloom.intf.PersistenceLayer;
import com.bloom.intf.PersistenceLayer.Range;
import com.bloom.waction.Waction;
import com.bloom.waction.WactionKey;
import com.bloom.recovery.Position;

import java.util.HashMap;
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

public class DefaultJPAPersistenceLayerImpl
  implements PersistenceLayer
{
  private static Logger logger = Logger.getLogger(DefaultJPAPersistenceLayerImpl.class);
  protected String persistenceUnitName = null;
  protected Map<String, Object> props = null;
  protected EntityManagerFactory factory;
  protected WactionStore ws = null;
  
  public DefaultJPAPersistenceLayerImpl() {}
  
  public DefaultJPAPersistenceLayerImpl(String persistenceUnit, Map<String, Object> properties)
  {
    this.persistenceUnitName = persistenceUnit;
    if (properties != null)
    {
      this.props = new HashMap();
      this.props.putAll(properties);
    }
  }
  
  public void setPersistenceUnitName(String persistenceUnitName)
  {
    this.persistenceUnitName = persistenceUnitName;
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
    throw new NotImplementedException();
  }
  
  protected synchronized EntityManager getEntityManager()
  {
    try
    {
      EntityManager entityManager = null;
      if ((this.props == null) || (this.props.isEmpty())) {
        entityManager = this.factory.createEntityManager();
      }
      return this.factory.createEntityManager(this.props);
    }
    catch (Exception ex)
    {
      throw new RuntimeException("Error establishing connection with persistence database for waction store: " + this.persistenceUnitName, ex);
    }
  }
  
  protected long T1 = 0L;
  protected long T2 = 0L;
  
  public PersistenceLayer.Range[] persist(Object object)
  {
    if (object == null) {
      throw new IllegalArgumentException("Cannot persist null object");
    }
    if (logger.isTraceEnabled()) {
      this.T1 = System.currentTimeMillis();
    }
    int noofinserts = 1;
    
    EntityTransaction txn = null;
    EntityManager entityManager = null;
    synchronized (this.factory)
    {
      try
      {
        entityManager = getEntityManager();
        txn = entityManager.getTransaction();
        txn.begin();
        if ((object instanceof List))
        {
          noofinserts = ((List)object).size();
          for (Object o : (List)object) {
            try
            {
              entityManager.persist(o);
            }
            catch (Exception ex)
            {
              logger.error("Error persisting object : " + o.getClass().getCanonicalName(), ex);
            }
          }
        }
        else
        {
          try
          {
            entityManager.persist(object);
          }
          catch (Exception ex)
          {
            logger.error("Error persisting object : " + object.getClass().getCanonicalName(), ex);
          }
        }
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
    if (logger.isTraceEnabled()) {
      this.T2 = System.currentTimeMillis();
    }
    if (logger.isTraceEnabled()) {
      logger.trace("persistence unit name  = " + this.persistenceUnitName + ", no-of-inserts = " + noofinserts + " and time taken : " + (this.T2 - this.T1) + " milliseconds");
    }
    return null;
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
  
  public void setStoreName(String storeName, String tableName)
  {
    setPersistenceUnitName(storeName);
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
      logger.error("Problem running query " + query + " with params " + params, e);
    }
    finally
    {
      entityManager.clear();
      entityManager.close();
    }
    return queryResults;
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
      logger.error("error executing native query : " + query);
    }
    finally
    {
      entityManager.clear();
      entityManager.close();
    }
    return result;
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
  
  public void close()
  {
    if (this.factory != null)
    {
      if (this.factory.isOpen()) {
        this.factory.close();
      }
      this.factory = null;
    }
    this.persistenceUnitName = null;
  }
  
  public Position getWSPosition(String namespaceName, String wactionStoreName)
  {
    throw new NotImplementedException("This method is not supported for DefaultJPAPersistenceLayerImpl");
  }
  
  public boolean clearWSPosition(String namespaceName, String wactionStoreName)
  {
    throw new NotImplementedException("This method is not supported for DefaultJPAPersistenceLayerImpl");
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
    return this.ws;
  }
  
  public void setWactionStore(WactionStore ws)
  {
    this.ws = ws;
  }
}
