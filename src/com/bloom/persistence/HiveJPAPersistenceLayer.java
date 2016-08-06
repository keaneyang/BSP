package com.bloom.persistence;

import java.util.List;
import java.util.Map;
import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;

import com.bloom.intf.PersistenceLayer;
import com.bloom.intf.PersistenceLayer.Range;

public class HiveJPAPersistenceLayer
  extends DefaultRuntimeJPAPersistenceLayerImpl
{
  private static Logger logger = Logger.getLogger(HiveJPAPersistenceLayer.class);
  
  public HiveJPAPersistenceLayer() {}
  
  public HiveJPAPersistenceLayer(String persistenceUnit, Map<String, Object> properties)
  {
    super(persistenceUnit, properties);
  }
  
  public int delete(Object object)
  {
    throw new NotImplementedException();
  }
  
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
        entityManager.flush();
      }
      finally
      {
        if (((txn == null) || (!txn.isActive())) || 
        
          (entityManager != null))
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
}
