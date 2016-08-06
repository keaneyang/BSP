package com.bloom.persistence;

import com.bloom.gen.RTMappingGenerator;
import com.bloom.gen.WAMetadataSource;
import com.bloom.intf.PersistenceLayer;
import com.bloom.intf.PersistenceLayer.Range;
import com.bloom.recovery.WactionStoreCheckpoint;
import com.bloom.runtime.NodeStartUp;
import com.bloom.utility.Utility;
import com.bloom.waction.Waction;
import com.bloom.waction.WactionContext;
import com.bloom.waction.WactionKey;
import com.bloom.recovery.Path;
import com.bloom.recovery.Position;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.persistence.Cache;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;
import javax.persistence.Query;
import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;
import org.eclipse.persistence.jpa.JpaEntityManagerFactory;
import org.eclipse.persistence.jpa.JpaHelper;

public class PersistenceLayerImpl
  implements PersistenceLayer
{
  private static Logger logger = Logger.getLogger(PersistenceLayerImpl.class);
  private EntityManagerFactory factory;
  private Map<String, Object> props = new HashMap();
  private WAMetadataSource mms = new WAMetadataSource();
  private String pu = "default";
  private String storeName = "default";
  private boolean metaDataRefreshed = false;
  private EntityManager em = null;
  private static Map<String, EntityManagerFactory> factories = new HashMap();
  
  private EntityManager getEntityManager(Map<String, Object> props)
  {
    this.factory = ((EntityManagerFactory)factories.get(this.pu));
    synchronized (factories)
    {
      if ((this.em == null) || (!this.em.isOpen()))
      {
        if (logger.isInfoEnabled()) {
          logger.info("Creating EntityManager with properties. Current storename : " + this.storeName);
        }
        this.em = ((EntityManagerFactory)factories.get(this.pu)).createEntityManager(props);
      }
    }
    return this.em;
  }
  
  public PersistenceLayerImpl(String pu, Map<String, Object> props)
  {
    this.pu = pu;
    if (props != null) {
      this.props.putAll(props);
    }
    synchronized (factories)
    {
      if (factories.get(pu) == null)
      {
        this.factory = Persistence.createEntityManagerFactory(pu, props);
        factories.put(pu, this.factory);
        if (logger.isInfoEnabled()) {
          logger.info("Created PersistenceLayerImpl. Total factories are : " + factories.size() + ".\npu= " + pu + "\nprop = " + props);
        }
      }
    }
  }
  
  public void init()
  {
    init(null);
  }
  
  public void init(String storeName)
  {
    if (logger.isTraceEnabled()) {
      logger.trace("init method called. storename = " + storeName);
    }
    String xml = RTMappingGenerator.getMappings(storeName);
    String mapping_file_name = NodeStartUp.getPlatformHome() + "/conf/eclipselink-orm-" + storeName + ".xml";
    File file = null;
    if (xml != null)
    {
      BufferedWriter writer = null;
      
      try
      {
        writer = new BufferedWriter(new FileWriter(new File(mapping_file_name)));
        writer.write(xml);
        try
        {
          if (writer != null) {
            writer.close();
          }
        }
        catch (IOException ie) {}
        file = new File(mapping_file_name);
      }
      catch (Exception e)
      {
        logger.error("error writing orm mapping. ", e);
      }
      finally
      {
        try
        {
          if (writer != null) {
            writer.close();
          }
        }
        catch (IOException ie) {}
      }
    }
   
    if (file.exists())
    {
      this.mms.setStoreName(storeName);
      this.props.put("eclipselink.metadata-source", this.mms);
      JpaHelper.getEntityManagerFactory((EntityManagerFactory)factories.get(this.pu)).refreshMetadata(this.props);
      this.em = getEntityManager(this.props);
    }
  }
  
  public int delete(Object object)
  {
    throw new NotImplementedException();
  }
  
  public PersistenceLayer.Range[] persist(Object object)
  {
    this.factory = ((EntityManagerFactory)factories.get(this.pu));
    int noofinserts = 0;
    PersistenceLayer.Range[] rangeArray = new PersistenceLayer.Range[1];
    long T1 = System.currentTimeMillis();
    synchronized (this.factory)
    {
      if (object == null)
      {
        logger.warn("Got null object to persist so, simply skip everything from here");
        return rangeArray;
      }
      if (logger.isTraceEnabled()) {
        logger.trace("Persisting object : " + object.getClass().getCanonicalName());
      }
      String mapping_file_name = NodeStartUp.getPlatformHome() + "/conf/eclipselink-orm-" + this.storeName + ".xml";
      if ((!this.metaDataRefreshed) && (((object instanceof Waction)) || ((object instanceof List))))
      {
        if (logger.isInfoEnabled()) {
          logger.info("metadata not refreshed. so doing it for WactionStore : " + this.storeName);
        }
        Waction wobject;
        if ((object instanceof Waction)) {
          wobject = (Waction)object;
        } else {
          wobject = (Waction)((List)object).get(0);
        }
        String xml = RTMappingGenerator.getMappings(wobject.getInternalWactionStoreName());
        if (logger.isDebugEnabled()) {
          logger.debug("xml mapping for event :\n" + xml);
        }
        BufferedWriter writer = null;
        try
        {
          writer = new BufferedWriter(new FileWriter(new File(mapping_file_name)));
          writer.write(xml);
          try
          {
            if (writer != null) {
              writer.close();
            }
          }
          catch (IOException ie) {}
          this.mms.setStoreName(this.storeName);
        }
        catch (Exception e)
        {
          logger.error("error writing orm mapping. ", e);
        }
        finally
        {
          try
          {
            if (writer != null) {
              writer.close();
            }
          }
          catch (IOException ie) {}
        }
        this.props.put("eclipselink.metadata-source", this.mms);
        
        this.factory = ((EntityManagerFactory)factories.get(this.pu));
        this.factory.getCache().evictAll();
        JpaHelper.getEntityManagerFactory((EntityManagerFactory)factories.get(this.pu)).refreshMetadata(this.props);
        this.metaDataRefreshed = true;
        if (logger.isDebugEnabled()) {
          logger.debug("copied orm mapping file. Refreshing metadata for persistence-unit : " + this.pu);
        }
      }
      if (logger.isTraceEnabled()) {
        logger.trace("persisting object of type : " + object.getClass().getCanonicalName() + "\nobject=" + object.toString());
      }
      EntityManager em = getEntityManager(this.props);
      em.clear();
      EntityTransaction txn = null;
      try
      {
        txn = em.getTransaction();
        txn.begin();
        if ((object instanceof List))
        {
          noofinserts = ((List)object).size();
          Object o = null;
          Position writeCheckpoint = new Position();
          Iterator iterator = ((List)object).iterator();
          while (iterator.hasNext())
          {
            o = iterator.next();
            try
            {
              em.persist(o);
              if ((o instanceof Waction))
              {
                Waction wactionObject = (Waction)o;
                writeCheckpoint.mergeHigherPositions(wactionObject.getPosition());
              }
            }
            catch (Exception ex)
            {
              logger.error("Error persisting object : " + o.getClass().getCanonicalName(), ex);
              throw ex;
            }
          }
          for (Path checkpointPath : writeCheckpoint.values())
          {
            if (logger.isDebugEnabled())
            {
              logger.debug("Writing checkpoint path:");
              Utility.prettyPrint(checkpointPath);
            }
            putPath(em, checkpointPath);
          }
        }
        else
        {
          try
          {
            em.persist(object);
            if ((object instanceof Waction))
            {
              Waction wactionObject = (Waction)object;
              for (Path checkpointPath : wactionObject.getPosition().values()) {
                putPath(em, checkpointPath);
              }
            }
          }
          catch (Exception ex)
          {
            logger.error("Error persisting object : " + object.getClass().getCanonicalName() + "," + ex.getLocalizedMessage());
            throw ex;
          }
        }
        txn.commit();
      }
      catch (Exception e)
      {
        e = 
        
          e;
        if ((txn != null) && (txn.isActive())) {
          txn.rollback();
        }
      }
      finally {}
    }
    long T2 = System.currentTimeMillis();
    if (logger.isTraceEnabled()) {
      logger.trace("no-of-inserts=" + noofinserts + " and time taken : " + (T2 - T1) + " milliseconds");
    }
    rangeArray[0] = new PersistenceLayer.Range(0, noofinserts);
    return rangeArray;
  }
  
  public void merge(Object object)
  {
    if (object == null)
    {
      logger.warn("Got null object to merge so, simply skip everything from here");
      return;
    }
    this.factory = ((EntityManagerFactory)factories.get(this.pu));
    synchronized (this.factory)
    {
      try
      {
        EntityManager em = getEntityManager(this.props);
        em.getTransaction().begin();
        em.merge(object);
        em.getTransaction().commit();
      }
      finally {}
    }
  }
  
  public Object get(Class<?> objectClass, Object objectId)
  {
    this.factory = ((EntityManagerFactory)factories.get(this.pu));
    synchronized (this.factory)
    {
      try
      {
        EntityManager em = getEntityManager(this.props);
        em.getTransaction().begin();
        Object result = em.find(objectClass, objectId);
        em.getTransaction().commit();
        Object localObject1 = result;return localObject1;
      }
      finally {}
    }
  }
  
  public void setStoreName(String storeName, String tableName)
  {
    this.storeName = storeName;
  }
  
  public int executeUpdate(String queryString, Map<String, Object> params)
  {
    this.factory = ((EntityManagerFactory)factories.get(this.pu));
    EntityManager em = getEntityManager(this.props);
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
    return -1;
  }
  
  public Object runNativeQuery(String query)
  {
    this.factory = ((EntityManagerFactory)factories.get(this.pu));
    synchronized (this.factory)
    {
      EntityManager em = getEntityManager(this.props);
      Query q = em.createNativeQuery(query, Waction.class);
      Waction w = null;
      try
      {
        w = (Waction)q.getSingleResult();
        Field[] fields = w.context.getClass().getDeclaredFields();
        for (Field f : fields) {
          w.context.put(f.getName(), f.get(w.context));
        }
      }
      catch (Exception e)
      {
        logger.error("Problem running native query: " + query, e);
      }
      return w;
    }
  }
  
  public List<?> runQuery(String queryString, Map<String, Object> params, Integer maxResults)
  {
    this.factory = ((EntityManagerFactory)factories.get(this.pu));
    synchronized (this.factory)
    {
      List<?> queryResults = null;
      try
      {
        EntityManager em = getEntityManager(this.props);
        Query query = em.createQuery(queryString);
        if ((maxResults != null) && (maxResults.intValue() > 0)) {
          query.setMaxResults(maxResults.intValue());
        }
        if (params != null) {
          for (String key : params.keySet()) {
            query.setParameter(key, params.get(key));
          }
        }
        queryResults = query.getResultList();
        for (Object oo : queryResults) {
          if ((oo instanceof Waction))
          {
            Waction w = (Waction)oo;
            Field[] fields = w.context.getClass().getDeclaredFields();
            for (Field f : fields) {
              w.context.put(f.getName(), f.get(w.context));
            }
          }
        }
      }
      catch (IllegalArgumentException|IllegalAccessException e)
      {
        logger.error("Problem running query " + queryString + " with params " + params, e);
      }
      finally {}
      return queryResults;
    }
  }
  
  public void close()
  {
    if (logger.isInfoEnabled()) {
      logger.info("Closing persistence layer object. ");
    }
    this.factory = ((EntityManagerFactory)factories.get(this.pu));
    synchronized (factories)
    {
      if (this.factory != null)
      {
        if (this.em != null)
        {
          if (logger.isDebugEnabled()) {
            logger.debug("EntityManager is not null. ");
          }
          if (this.em.isOpen()) {
            this.em.close();
          }
          this.em = null;
        }
        if (this.factory.isOpen()) {
          this.factory.close();
        }
        factories.remove(this.pu);
        this.factory = null;
      }
    }
  }
  
  private static void putPath(EntityManager em, Path path)
  {
    if (logger.isTraceEnabled()) {
      logger.trace("StatusDataStore.putPath(" + path + ", " + em + ");");
    }
    String query = "SELECT cp FROM WactionStoreCheckpoint cp WHERE cp.pathItems=:pathItems";
    
    Query q = em.createQuery(query);
    q.setParameter("pathItems", path.getPathItems());
    q.setMaxResults(1);
    List<?> result = q.getResultList();
    
    WactionStoreCheckpoint checkpoint = new WactionStoreCheckpoint(path.getPathItems(), path.getSourcePosition());
    if ((result == null) || (result.size() == 0))
    {
      em.persist(checkpoint);
    }
    else
    {
      checkpoint.id = ((WactionStoreCheckpoint)result.get(0)).id;
      em.merge(checkpoint);
    }
  }
  
  public Position getWSPosition(String namespaceName, String wactionStoreName)
  {
    String fullTableName = namespaceName + "_" + wactionStoreName;
    
    init(fullTableName);
    String query = "SELECT cp FROM WactionStoreCheckpoint cp";
    EntityManager em = getEntityManager(this.props);
    em.getTransaction().begin();
    
    Query q = em.createQuery(query);
    q.setMaxResults(1);
    em.getTransaction().commit();
    List<?> result = q.getResultList();
    if (result == null) {
      return new Position();
    }
    Set<Path> paths = new HashSet();
    for (Object o : result)
    {
      WactionStoreCheckpoint wactionStoreCheckpoint = (WactionStoreCheckpoint)o;
      Path path = new Path(wactionStoreCheckpoint.pathItems, wactionStoreCheckpoint.sourcePosition);
      paths.add(path);
    }
    Position wactionStorePosition = new Position(paths);
    return wactionStorePosition;
  }
  
  public boolean clearWSPosition(String namespaceName, String wactionStoreName)
  {
    String fullTableName = namespaceName + "_" + wactionStoreName;
    
    init(fullTableName);
    String query = "DELETE FROM WactionStoreCheckpoint";
    EntityManager em = this.factory.createEntityManager(this.props);
    em.getTransaction().begin();
    
    Query q = em.createQuery(query);
    
    int numDeleted = q.executeUpdate();
    em.getTransaction().commit();
    if (Logger.getLogger("Recovery").isDebugEnabled()) {
      Logger.getLogger("Recovery").debug("WactionStore cleared " + numDeleted + " checkpoint paths");
    }
    return true;
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
