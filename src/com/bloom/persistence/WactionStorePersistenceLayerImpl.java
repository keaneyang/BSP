package com.bloom.persistence;

import com.bloom.intf.PersistenceLayer;
import com.bloom.intf.PersistenceLayer.Range;
import com.bloom.recovery.WactionStoreCheckpoint;
import com.bloom.uuid.UUID;
import com.bloom.waction.Waction;
import com.bloom.waction.WactionContext;
import com.bloom.recovery.Path;
import com.bloom.recovery.Path.ItemList;
import com.bloom.recovery.Position;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import javax.persistence.Cache;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Query;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;

public class WactionStorePersistenceLayerImpl
  extends DefaultRuntimeJPAPersistenceLayerImpl
{
  private static Logger logger = Logger.getLogger(WactionStorePersistenceLayerImpl.class);
  private String storeName = null;
  private String contextTableName = null;
  private EntityManagerPool emPool = null;
  private int batchWritingSize;
  
  private class EntityManagerPool
  {
    WactionStorePersistenceLayerImpl parent;
    int numEMs = 10;
    private EntityManager[] emArray;
    private boolean[] emsAvailable;
    
    EntityManagerPool(Map<String, Object> par)
    {
      this.parent = par;
      try
      {
        this.numEMs = Integer.parseInt((String)p.get("MAX_THREAD_NUM"));
      }
      catch (Exception ne) {}
      this.emArray = new EntityManager[this.numEMs];
      this.emsAvailable = new boolean[this.numEMs];
      for (int i = 0; i < this.numEMs; i++) {
        this.emsAvailable[i] = true;
      }
    }
    
    public int getAvailableEMEntry()
    {
      synchronized (this.parent)
      {
        for (int i = 0; i < this.numEMs; i++) {
          if (this.emsAvailable[i] == 1)
          {
            this.emsAvailable[i] = false;
            return i;
          }
        }
      }
      return -1;
    }
    
    public EntityManager getEmAt(int i)
    {
      synchronized (this.parent)
      {
        if (this.emArray[i] == null) {
          if ((WactionStorePersistenceLayerImpl.this.props == null) || (WactionStorePersistenceLayerImpl.this.props.isEmpty())) {
            this.emArray[i] = WactionStorePersistenceLayerImpl.this.factory.createEntityManager();
          } else {
            this.emArray[i] = WactionStorePersistenceLayerImpl.this.factory.createEntityManager(WactionStorePersistenceLayerImpl.this.props);
          }
        }
      }
      return this.emArray[i];
    }
    
    public void releaseEmAt(int i)
    {
      synchronized (this.parent)
      {
        if ((this.emArray[i] != null) && (this.emArray[i].isOpen()))
        {
          this.emArray[i].clear();
          this.emArray[i].close();
        }
        this.emsAvailable[i] = true;
      }
    }
  }
  
  public WactionStorePersistenceLayerImpl(String persistenceUnit, Map<String, Object> properties)
  {
    super(persistenceUnit, properties);
    this.storeName = persistenceUnit;
    try
    {
      this.batchWritingSize = com.bloom.wactionstore.Utility.extractInt(properties.get("BATCH_WRITING_SIZE"));
    }
    catch (Exception e)
    {
      this.batchWritingSize = 10000;
    }
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
  
  private class PersistParallel
    implements Runnable
  {
    public int indexUsed = -1;
    public EntityManager entityManager = null;
    public EntityTransaction txn = null;
    Object[] objList = null;
    CountDownLatch cLatch = null;
    Exception thException = null;
    public final Position writeCheckpoint = new Position();
    public List subList = null;
    public boolean doCommit = false;
    
    public PersistParallel(Object[] arr, EntityManager em, int index, CountDownLatch ctl, boolean commit)
    {
      this.indexUsed = index;
      this.entityManager = em;
      this.objList = ((Object[])arr.clone());
      this.cLatch = ctl;
      this.doCommit = commit;
    }
    
    public void run()
    {
      Object o = null;
      try
      {
        this.txn = this.entityManager.getTransaction();
        this.txn.begin();
        for (int i = 0; i < this.objList.length; i++)
        {
          o = this.objList[i];
          
          this.entityManager.persist(o);
          if ((o instanceof Waction))
          {
            if (WactionStorePersistenceLayerImpl.logger.isTraceEnabled()) {
              WactionStorePersistenceLayerImpl.logger.trace("Writing waction to disk: " + o);
            }
            Waction wactionObject = (Waction)o;
            this.writeCheckpoint.mergeHigherPositions(wactionObject.getPosition());
          }
        }
        if (WactionStorePersistenceLayerImpl.logger.isDebugEnabled()) {
          WactionStorePersistenceLayerImpl.logger.debug("about to commit txn : indexUsed= " + this.indexUsed);
        }
        if (this.doCommit) {
          this.txn.commit();
        }
      }
      catch (Exception ex)
      {
        WactionStorePersistenceLayerImpl.logger.warn("Error persisting object : " + o.getClass().getCanonicalName() + "\nReason: " + ex.getLocalizedMessage());
        this.thException = ex;
        if ((this.doCommit) && (this.txn != null) && (this.txn.isActive()))
        {
          WactionStorePersistenceLayerImpl.logger.warn("Exception persisting. So rolling back transaction.");
          this.txn.rollback();
        }
      }
      finally
      {
        this.cLatch.countDown();
      }
    }
  }
  
  public int delete(Object object)
  {
    throw new NotImplementedException();
  }
  
  public PersistenceLayer.Range[] persist(Object object)
  {
    int numWritten = 0;
    PersistenceLayer.Range[] successRanges = null;
    
    int batchsize = this.batchWritingSize;
    int numThreads = 0;
    if (object == null) {
      throw new IllegalArgumentException("Cannot persist null object");
    }
    if (logger.isDebugEnabled()) {
      this.T1 = System.currentTimeMillis();
    }
    int noofinserts = 1;
    this.emPool = new EntityManagerPool( this.props);
    synchronized (this.factory)
    {
      this.factory.getCache().evictAll();
      if ((object instanceof List))
      {
        boolean doCommit = false;
        ArrayList<Integer> emEntries = new ArrayList();
        
        noofinserts = ((List)object).size();
        
        int emIndex = -1;
        while ((emIndex = this.emPool.getAvailableEMEntry()) == -1) {
          try
          {
            Thread.sleep(10L);
          }
          catch (InterruptedException e) {}
        }
        emEntries.add(Integer.valueOf(emIndex));
        while ((emIndex = this.emPool.getAvailableEMEntry()) != -1) {
          emEntries.add(Integer.valueOf(emIndex));
        }
        numThreads = Math.min(noofinserts / batchsize + 1, emEntries.size());
        if (emEntries.size() > numThreads) {
          for (emIndex = emEntries.size() - 1; emIndex > numThreads - 1; emIndex--) {
            this.emPool.releaseEmAt(((Integer)emEntries.get(emIndex)).intValue());
          }
        }
        batchsize = noofinserts / numThreads;
        PersistParallel[] ppArray = new PersistParallel[numThreads];
        Thread[] t = new Thread[numThreads];
        successRanges = new PersistenceLayer.Range[numThreads];
        CountDownLatch ctl = new CountDownLatch(numThreads);
        
        Waction w = (Waction)((List)object).get(0);
        if (w.getPosition() == null) {
          doCommit = true;
        }
        if (logger.isDebugEnabled()) {
          logger.debug("no of wactions= " + noofinserts + ", num of parallel threads= " + numThreads + ", batchsize= " + batchsize + ", is recovery on? " + (!doCommit));
        }
        for (int i = 0; i < numThreads; i++)
        {
          int startIndex = i * batchsize;
          int endIndex = 0;
          if (i == numThreads - 1) {
            endIndex = noofinserts;
          } else {
            endIndex = (i + 1) * batchsize > noofinserts ? noofinserts : (i + 1) * batchsize;
          }
          synchronized (object)
          {
            List subList = ((List)object).subList(startIndex, endIndex);
            EntityManager entityManager = this.emPool.getEmAt(((Integer)emEntries.get(i)).intValue());
            entityManager.clear();
            ppArray[i] = new PersistParallel(subList.toArray(), entityManager, ((Integer)emEntries.get(i)).intValue(), ctl, doCommit);
            ppArray[i].subList = subList;
            successRanges[i] = new PersistenceLayer.Range(startIndex, endIndex);
          }
          t[i] = new Thread(ppArray[i], "PersistParallelThread@" + this.storeName);
          t[i].start();
        }
        try
        {
          ctl.await();
        }
        catch (InterruptedException e)
        {
          logger.warn("waction write thread was interrupted due to " + e.getLocalizedMessage());
        }
        if (logger.isDebugEnabled()) {
          logger.debug("All PersistParallel threads finished processing.");
        }
        checkForExceptions(ppArray);
        for (int ik = 0; ik < numThreads; ik++) {
          if (ppArray[ik].thException == null) {
            successRanges[ik].setSuccessful(true);
          } else {
            if (!doCommit) {
              break;
            }
          }
        }
        for (int i = 0; i < numThreads; i++) {
          if (ppArray[i].thException == null)
          {
            if (!doCommit)
            {
              putPath(ppArray[i].entityManager, ppArray[i].writeCheckpoint);
              ppArray[i].txn.commit();
            }
            this.emPool.releaseEmAt(ppArray[i].indexUsed);
            for (int k = 0; k < ppArray[i].objList.length; k++)
            {
              Waction o = (Waction)ppArray[i].objList[k];
              o.designatePersisted();
            }
            numWritten += ppArray[i].objList.length;
          }
          else
          {
            if (!doCommit)
            {
              for (int ij = i; ij < numThreads; ij++)
              {
                if ((!doCommit) && (ppArray[ij].txn != null) && (ppArray[ij].txn.isActive())) {
                  ppArray[ij].txn.rollback();
                }
                this.emPool.releaseEmAt(ppArray[ij].indexUsed);
              }
              break;
            }
            if ((ppArray[i].txn != null) && (ppArray[i].txn.isActive())) {
              ppArray[i].txn.rollback();
            }
            this.emPool.releaseEmAt(ppArray[i].indexUsed);
          }
        }
      }
      else
      {
        int i = this.emPool.getAvailableEMEntry();
        EntityManager em = this.emPool.getEmAt(i);
        em.clear();
        EntityTransaction txn = null;
        try
        {
          txn = em.getTransaction();
          txn.begin();
          
          em.persist(object);
          if ((object instanceof Waction))
          {
            if (logger.isDebugEnabled()) {
              logger.debug("Writing waction to disk: " + object);
            }
            Waction wactionObject = (Waction)object;
            Position position = wactionObject.getPosition();
            if (position != null) {
              putPath(em, position);
            }
            wactionObject.designatePersisted();
          }
          txn.commit();
          numWritten++;
        }
        catch (Exception ex)
        {
          logger.error("Error persisting object : " + object.getClass().getCanonicalName(), ex);
          if ((txn != null) && (txn.isActive())) {
            txn.rollback();
          }
        }
        this.emPool.releaseEmAt(i);
      }
      this.factory.getCache().evictAll();
    }
    closeEmPool();
    if (logger.isDebugEnabled()) {
      this.T2 = System.currentTimeMillis();
    }
    if (logger.isDebugEnabled()) {
      logger.debug("ws = " + this.persistenceUnitName + ", inserts = " + numWritten + " and time : " + (this.T2 - this.T1) + " milli sec, rate(inserts per sec) : " + Math.floor(numWritten * 1000 / (this.T2 - this.T1)));
    }
    return successRanges;
  }
  
  private void checkForExceptions(PersistParallel[] ppArray)
  {
    for (int ik = 0; ik < ppArray.length; ik++) {
      if (ppArray[ik].thException != null)
      {
        if (logger.isInfoEnabled()) {
          logger.info("Found exception : " + ppArray[ik].thException.getClass().getCanonicalName() + ". so setting isCrashed to true");
        }
        this.ws.isCrashed = true;
        this.ws.crashCausingException = ppArray[ik].thException;
        return;
      }
    }
    this.ws.isCrashed = false;
    this.ws.crashCausingException = null;
  }
  
  private void closeEmPool()
  {
    if (this.emPool != null)
    {
      if (logger.isDebugEnabled()) {
        logger.debug(this.emPool.numEMs + " - closing all entity managers.");
      }
      for (int i = 0; i < this.emPool.numEMs; i++) {
        if ((this.emPool.emArray[i] != null) && (this.emPool.emArray[i].isOpen()))
        {
          this.emPool.emArray[i].clear();
          this.emPool.emArray[i].close();
          this.emPool.emArray[i] = null;
        }
      }
    }
    this.emPool = null;
  }
  
  public void setStoreName(String storeName, String tableName)
  {
    this.storeName = storeName;
    this.contextTableName = tableName;
  }
  
  public int executeUpdate(String queryString, Map<String, Object> params)
  {
    EntityManager entityManager = null;
    EntityTransaction txn = null;
    int queryResults = -1;
    synchronized (this.factory)
    {
      try
      {
        entityManager = getEntityManager();
        txn = entityManager.getTransaction();
        txn.begin();
        Query query = entityManager.createNativeQuery(queryString);
        if (params != null) {
          for (String key : params.keySet()) {
            query.setParameter(key, params.get(key));
          }
        }
        entityManager.getTransaction().begin();
        queryResults = query.executeUpdate();
        txn.commit();
      }
      finally
      {
        if (txn.isActive()) {
          txn.rollback();
        }
        entityManager.clear();
        entityManager.close();
      }
      return queryResults;
    }
  }
  
  public Object runNativeQuery(String queryStr)
  {
    synchronized (this.factory)
    {
      EntityManager entityManager = getEntityManager();
      Query query = entityManager.createNativeQuery(queryStr, Waction.class);
      Waction w = null;
      try
      {
        w = (Waction)query.getSingleResult();
        Field[] fields = w.context.getClass().getDeclaredFields();
        for (Field f : fields) {
          w.context.put(f.getName(), f.get(w.context));
        }
      }
      catch (Exception e)
      {
        e.printStackTrace();
      }
      finally
      {
        entityManager.clear();
        entityManager.close();
      }
      return w;
    }
  }
  
  public List<?> runQuery(String queryString, Map<String, Object> params, Integer maxResults)
  {
    synchronized (this.factory)
    {
      List<?> queryResults = null;
      EntityManager entityManager = null;
      try
      {
        entityManager = getEntityManager();
        Query query = entityManager.createQuery(queryString);
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
        e.printStackTrace();
      }
      finally
      {
        entityManager.clear();
        entityManager.close();
      }
      return queryResults;
    }
  }
  
  public <T extends Waction> Iterable<T> getResults(Class<T> objectClass, WactionQuery query)
  {
    String tableName = this.contextTableName;
    if (logger.isDebugEnabled()) {
      logger.debug("trying to get results from table :" + tableName + ", and create objects of type : " + objectClass.getCanonicalName());
    }
    String qry = null;
    synchronized (this.factory)
    {
      try
      {
        EntityManager entityManager = getEntityManager();
        String jdbcUrl = (String)entityManager.getProperties().get("JDBC_URL");
        boolean supportsArrayIn = true;
        if (jdbcUrl.toLowerCase().contains(":derby:")) {
          supportsArrayIn = false;
        }
        if ((query.willGetLatestPerKey()) && (!query.willGetEvents()))
        {
          if (supportsArrayIn)
          {
            String subQry = "SELECT W.partitionKey, MAX(W.wactionTs) FROM " + tableName + " W " + query.getWhere("W", true) + " GROUP BY W.partitionKey";
            qry = "SELECT * FROM " + tableName + " WA WHERE (WA.partitionKey, WA.wactionTs) IN (" + subQry + ")";
            
            Query dbQuery = entityManager.createNativeQuery(qry, objectClass);
            return new JPAIterable(entityManager, dbQuery, true, true);
          }
          String subQry = "SELECT W.key, MAX(W.wactionTs) FROM " + tableName + " W " + query.getWhere("W", false) + " GROUP BY W.key";
          Query subDbQuery = entityManager.createQuery(subQry);
          List<Object[]> subs = subDbQuery.getResultList();
          if ((subs != null) && (!subs.isEmpty()))
          {
            List<T> results = new ArrayList();
            
            CriteriaBuilder cb = entityManager.getCriteriaBuilder();
            CriteriaQuery<Object> cq = cb.createQuery();
            Root<T> e = cq.from(objectClass);
            cq.where(cb.equal(e.get("key"), cb.parameter(String.class, "key")));
            cq.where(cb.equal(e.get("wactionTs"), cb.parameter(Long.class, "wactionTs")));
            
            Query dbQuery = entityManager.createQuery(cq);
            dbQuery.setHint("eclipselink.fetch-group.name", "noEvents");
            for (Object[] sub : subs)
            {
              dbQuery.setParameter("key", sub[0]);
              dbQuery.setParameter("wactionTs", sub[1]);
              
              List<T> res = dbQuery.getResultList();
              if ((res != null) && (!res.isEmpty()))
              {
                T latest = null;
                for (T w : res) {
                  if (query.matches(w)) {
                    if (latest == null) {
                      latest = w;
                    } else if (w.getUuid().compareTo(latest.getUuid()) > 0) {
                      latest = w;
                    }
                  }
                }
                results.add(latest);
              }
            }
            return results;
          }
          return Collections.emptyList();
        }
        qry = "SELECT W FROM " + tableName + " W " + query.getWhere("W", false);
        
        Query dbQuery = entityManager.createQuery(qry);
        return new JPAIterable(entityManager, dbQuery, false, !query.willGetEvents());
      }
      catch (Throwable t)
      {
        logger.error("Problem executing db query: " + qry, t);
        throw t;
      }
    }
  }
  
  public <T extends Waction> Iterable<T> getResults(Class<T> objectClass, String wactionKey, WactionQuery query)
  {
    if (logger.isDebugEnabled()) {
      logger.debug("trying to get results for store :" + this.storeName + ", and create objects of type : " + objectClass.getCanonicalName() + ", with key :" + wactionKey);
    }
    List<T> results = null;
    synchronized (this.factory)
    {
      EntityManager entityManager = getEntityManager();
      
      CriteriaBuilder cb = entityManager.getCriteriaBuilder();
      CriteriaQuery<Object> cq = cb.createQuery();
      Root<T> e = cq.from(objectClass);
      cq.where(cb.equal(e.get("mapKey"), cb.parameter(String.class, "mapKey")));
      
      Query dbQuery = entityManager.createQuery(cq);
      dbQuery.setParameter("mapKey", wactionKey);
      
      return new JPAIterable(entityManager, dbQuery, false, false);
    }
  }
  
  public void close()
  {
    if (this.factory != null) {
      synchronized (this.factory)
      {
        closeEmPool();
        if (this.factory != null)
        {
          if (this.factory.isOpen()) {
            this.factory.close();
          }
          this.factory = null;
          if (logger.isInfoEnabled()) {
            logger.info("Closed persistence layer object for pu : " + this.persistenceUnitName);
          }
        }
      }
    }
    this.persistenceUnitName = null;
  }
  
  private static void putPath(EntityManager em, Position writeCheckpoint)
  {
    if (logger.isDebugEnabled())
    {
      logger.debug("Persist WactionStoreCheckpoint: " + writeCheckpoint);
      com.bloom.utility.Utility.prettyPrint(writeCheckpoint);
    }
    HashMap<String, WactionStoreCheckpoint> seenPaths = new HashMap();
    for (Path path : writeCheckpoint.values())
    {
      WactionStoreCheckpoint checkpoint = (WactionStoreCheckpoint)em.find(WactionStoreCheckpoint.class, WactionStoreCheckpoint.getHash(path.getPathItems()));
      if (checkpoint == null)
      {
        if ((checkpoint = (WactionStoreCheckpoint)seenPaths.get(path.getPathItems().toString())) == null)
        {
          checkpoint = new WactionStoreCheckpoint(path.getPathItems(), path.getSourcePosition());
          em.persist(checkpoint);
          seenPaths.put(path.getPathItems().toString(), checkpoint);
        }
        else
        {
          checkpoint.sourcePosition = path.getSourcePosition();
          em.merge(checkpoint);
        }
      }
      else
      {
        checkpoint.sourcePosition = path.getSourcePosition();
        em.merge(checkpoint);
      }
    }
  }
  
  public Position getWSPosition(String namespaceName, String wactionStoreName)
  {
    String fullTableName = namespaceName + "_" + wactionStoreName;
    
    init(fullTableName);
    String query = "SELECT cp FROM WactionStoreCheckpoint cp";
    EntityManager em = getEntityManager();
    em.getTransaction().begin();
    
    Query q = em.createQuery(query);
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
}
