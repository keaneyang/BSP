package com.bloom.metaRepository;

import com.bloom.runtime.Server;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Flow;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.utils.NamePolicy;
import com.bloom.uuid.UUID;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.sql.SQLNonTransientConnectionException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;
import javax.persistence.PersistenceException;
import javax.persistence.Query;
import javax.persistence.TypedQuery;
import org.apache.log4j.Logger;
import org.eclipse.persistence.exceptions.DatabaseException;

public class MetaDataDBOps
{
  private static final Logger logger = Logger.getLogger(MetaDataDBOps.class);
  private static MetaDataDBOps INSTANCE = new MetaDataDBOps();
  private static final String PU_NAME = "MetadataStore";
  public static final String LOGGING_LEVEL = "SEVERE";
  public static String DBLocation = null;
  public static String DBName = null;
  public static String DBUname = null;
  public static String DBPassword = null;
  private static volatile EntityManager instance;
  
  private MetaDataDBOps()
  {
    if (INSTANCE != null) {
      throw new IllegalStateException("An instance of MetaDataDBOps already exists");
    }
  }
  
  public static MetaDataDBOps getInstance()
  {
    return INSTANCE;
  }
  
  public static void setDBDetails(String DBLocation, String DBName, String DBUname, String DBPassword)
  {
    DBLocation = DBLocation;
    DBName = DBName;
    DBUname = DBUname;
    DBPassword = DBPassword;
    System.out.println("DB details : " + DBLocation + " , " + DBName + " , " + DBUname);
  }
  
  private static EntityManager initManager()
  {
    try
    {
      Map<String, Object> properties = new HashMap();
      
      MetaDataDbProvider metaDataDbProvider = Server.getMetaDataDBProviderDetails();
      properties.put("javax.persistence.jdbc.user", DBUname);
      properties.put("javax.persistence.jdbc.password", DBPassword);
      properties.put("javax.persistence.jdbc.url", metaDataDbProvider.getJDBCURL(DBLocation, DBName, DBUname, DBPassword));
      properties.put("javax.persistence.jdbc.driver", metaDataDbProvider.getJDBCDriver());
      properties.put("eclipselink.logging.level", "SEVERE");
      properties.put("eclipselink.weaving", "STATIC");
      
      EntityManagerFactory m_factory = Persistence.createEntityManagerFactory("MetadataStore", properties);
      
      return m_factory.createEntityManager();
    }
    catch (Exception e)
    {
      logger.error(e, e);
      if ((e instanceof PersistenceException))
      {
        logger.error("Shutting down server it couldn't connect to database with error: " + e.getMessage());
        System.exit(1);
      }
      throw e;
    }
  }
  
  public static EntityManager getManager()
  {
    if (instance == null) {
      synchronized (MetaDataDBOps.class)
      {
        if (instance == null) {
          instance = initManager();
        }
      }
    }
    return instance;
  }
  
  private Object cloneObject(Object obj)
  {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    byte[] b;
    try
    {
      ObjectOutputStream oos = new ObjectOutputStream(bos);
      
      oos.writeObject(obj);
      oos.close();
      b = bos.toByteArray();
    }
    catch (IOException e)
    {
      b = null;
    }
    Object retObj = null;
    if (b != null)
    {
      ByteArrayInputStream bis = new ByteArrayInputStream((byte[])b);
      try
      {
        ObjectInputStream ois = new ObjectInputStream(bis);
        retObj = ois.readObject();
      }
      catch (ClassNotFoundException|IOException e)
      {
        retObj = null;
      }
    }
    return retObj;
  }
  
  public synchronized boolean store(MetaInfo.MetaObject mObject)
  {
    long opStartTime = 0L;long opEndTime = 0L;
    if (!Server.persistenceIsEnabled()) {
      return false;
    }
    EntityTransaction txn = null;
    
    mObject.setUri(mObject.getUri());
    
    mObject = (MetaInfo.MetaObject)cloneObject(mObject);
    if (mObject.type.ordinal() == EntityType.APPLICATION.ordinal()) {
      mObject.type = EntityType.FLOW;
    }
    EntityManager m = getManager();
    try
    {
      if (logger.isInfoEnabled()) {
        opStartTime = System.currentTimeMillis();
      }
      synchronized (m)
      {
        m.clear();
        txn = m.getTransaction();
        txn.begin();
        if (logger.isTraceEnabled()) {
          logger.trace("Store object " + mObject.getUri() + " = " + mObject + " in DB");
        }
        m.merge(mObject);
        m.flush();
        
        txn.commit();
      }
    }
    catch (Exception e)
    {
      logger.error("Problem storing object " + mObject.getUri() + " = " + mObject + " in DB", e);
      if ((e instanceof DatabaseException))
      {
        if ((((DatabaseException)e).getDatabaseErrorCode() == 40000) && ((((DatabaseException)e).getInternalException() instanceof SQLNonTransientConnectionException)))
        {
          logger.error("Shutting down server becasue it could not connect to dataabase with error code: " + ((DatabaseException)e).getDatabaseErrorCode(), e);
          System.exit(1);
        }
      }
      else if ((e instanceof PersistenceException))
      {
        logger.error("Shutting down server becasue it could not connect to dataabase with error ", e);
        System.exit(1);
      }
      else
      {
        logger.error("Error while writing to metadata database", e);
        if ((txn != null) && (txn.isActive())) {
          txn.rollback();
        }
      }
      throw e;
    }
    finally
    {
      if (logger.isDebugEnabled())
      {
        opEndTime = System.currentTimeMillis();
        logger.info("DB Operation for put of " + mObject.getFullName() + " took " + (opEndTime - opStartTime) / 1000L + " seconds");
      }
    }
    return true;
  }
  
  public Object get(EntityType eType, UUID uuid, String namespace, String name, Integer version, MDConstants.typeOfGet get)
    throws MetaDataDBOpsException
  {
    Object result = null;
    switch (get)
    {
    case BY_UUID: 
      result = loadByUUID(uuid);
      break;
    case BY_NAME: 
      result = loadByName(eType, namespace, name, version);
      break;
    case BY_ENTITY_TYPE: 
      result = loadByEntityType(eType);
      break;
    case BY_NAMESPACE: 
      result = loadByNamespace(namespace);
      break;
    case BY_NAMESPACE_AND_ENTITY_TYPE: 
      result = loadByEntityTypeInNamespace(namespace, eType);
      break;
    case BY_ALL: 
      result = loadAll();
    }
    return result;
  }
  
  public Integer remove(EntityType eType, UUID uuid, String namespace, String name, Integer version, MDConstants.typeOfRemove remove)
    throws MetaDataDBOpsException
  {
    Integer result = Integer.valueOf(0);
    switch (remove)
    {
    case BY_UUID: 
      result = removeByUUID(uuid);
      break;
    case BY_NAME: 
      result = removeByName(eType, namespace, name, version);
    }
    return result;
  }
  
  public boolean checkIfFlowIsApplication(MetaInfo.MetaObject flow)
  {
    if (flow.getUri().contains(":APPLICATION:")) {
      return true;
    }
    return false;
  }
  
  public MetaInfo.MetaObject convertFlowToApplication(MetaInfo.MetaObject flow)
  {
    MetaInfo.MetaObject oldmeta = flow;
    try
    {
      flow = oldmeta.clone();
    }
    catch (CloneNotSupportedException e) {}
    flow.type = EntityType.APPLICATION;
    return flow;
  }
  
  private synchronized List<MetaInfo.MetaObject> loadAll()
  {
    List<MetaInfo.MetaObject> resultMetaObjects = null;
    try
    {
      if (logger.isDebugEnabled()) {
        logger.debug("Load All");
      }
      EntityManager manager = getManager();
      synchronized (manager)
      {
        String query = "Select w from MetaInfo$MetaObject w ";
        Query q = manager.createQuery(query);
        List<?> resultList = q.getResultList();
        if (resultList != null)
        {
          if (logger.isDebugEnabled()) {
            logger.debug("Found " + resultList.size() + " objects in Database.");
          }
          resultMetaObjects = new ArrayList();
          for (Object o : resultList)
          {
            MetaInfo.MetaObject metaObject = (MetaInfo.MetaObject)o;
            if (metaObject.getType().ordinal() == EntityType.FLOW.ordinal())
            {
              if (checkIfFlowIsApplication(metaObject))
              {
                MetaInfo.MetaObject converted = convertFlowToApplication(metaObject);
                resultMetaObjects.add(converted);
              }
              else
              {
                resultMetaObjects.add(metaObject);
              }
            }
            else {
              resultMetaObjects.add(metaObject);
            }
          }
        }
        else if (logger.isDebugEnabled())
        {
          logger.debug("No Objects found in Database.");
        }
      }
    }
    catch (Exception e)
    {
      if ((e instanceof DatabaseException))
      {
        if ((((DatabaseException)e).getDatabaseErrorCode() == 40000) && ((((DatabaseException)e).getInternalException() instanceof SQLNonTransientConnectionException)))
        {
          logger.error("Shutting down server becasue it could not connect to database with error code: " + ((DatabaseException)e).getDatabaseErrorCode(), e);
          System.exit(1);
        }
      }
      else if ((e instanceof PersistenceException))
      {
        logger.error("Shutting down server becasue it could not connect to dataabase with error", e);
        System.exit(1);
      }
    }
    return resultMetaObjects;
  }
  
  private synchronized MetaInfo.MetaObject loadByUUID(UUID key)
    throws MetaDataDBOpsException
  {
    MetaInfo.MetaObject mObject = null;
    try
    {
      MDConstants.checkNullParams("Can't load MetaObject with NULL UUID", new Object[] { key });
      if (logger.isDebugEnabled()) {
        logger.debug("Load Meta Object by UUID " + key);
      }
      EntityManager manager = getManager();
      synchronized (manager)
      {
        String query = "Select w from MetaInfo$MetaObject w where w.uuid = :uuid";
        
        TypedQuery<Object[]> q = manager.createQuery(query, Object[].class);
        q.setParameter("uuid", key);
        
        List<?> wtypes = q.getResultList();
        if (wtypes.size() == 1)
        {
          mObject = (MetaInfo.MetaObject)wtypes.get(0);
          if (logger.isDebugEnabled()) {
            logger.debug("Loaded MetaObject by UUID" + key + " = " + mObject.getUuid());
          }
        }
        else if (logger.isDebugEnabled())
        {
          logger.debug("Could not load MetaObject by UUID " + key);
        }
        if ((mObject instanceof MetaInfo.Flow))
        {
          boolean isApp = checkIfFlowIsApplication(mObject);
          if (isApp) {
            mObject = convertFlowToApplication(mObject);
          }
        }
      }
    }
    catch (Exception e)
    {
      if ((e instanceof MetaDataDBOpsException)) {
        throw new MetaDataDBOpsException(e.getMessage());
      }
      if ((e instanceof DatabaseException))
      {
        if ((((DatabaseException)e).getDatabaseErrorCode() == 40000) && ((((DatabaseException)e).getInternalException() instanceof SQLNonTransientConnectionException)))
        {
          logger.error("Shutting down server becasue it could not connect to database with error code: " + ((DatabaseException)e).getDatabaseErrorCode(), e);
          System.exit(1);
        }
      }
      else if ((e instanceof PersistenceException))
      {
        logger.error("Shutting down server becasue it could not connect to dataabase with error", e);
        System.exit(1);
      }
    }
    return mObject;
  }
  
  private MetaInfo.MetaObject loadByName(EntityType eType, String namespace, String name, Integer version)
    throws MetaDataDBOpsException
  {
    try
    {
      MDConstants.checkNullParams("Can't load MetaObject with NULL values for either of, Entity Type: " + eType + ", Namespace: " + namespace + ", name: " + name, new Object[] { eType, namespace, name });
      
      List<MetaInfo.MetaObject> mObject = null;
      if (logger.isDebugEnabled()) {
        logger.debug("Load MetaObject by with name : " + name + " in namespace : " + namespace);
      }
      EntityManager m = getManager();
      synchronized (m)
      {
        Query q;
        if (version == null)
        {
          String query = "select w from MetaInfo$MetaObject w where w.name = :name AND w.type = :type AND w.nsName = :namespace ORDER BY w.version DESC";
           q = m.createQuery(query);
          q.setParameter("name", name);
          if (eType.ordinal() == EntityType.APPLICATION.ordinal()) {
            q.setParameter("type", EntityType.FLOW);
          } else {
            q.setParameter("type", eType);
          }
          q.setParameter("namespace", namespace);
        }
        else
        {
          String query = "select w from MetaInfo$MetaObject w where w.uri = :uri";
          q = m.createQuery(query);
          
          String uriInCaps = NamePolicy.makeKey(namespace + ":" + eType + ":" + name + ":" + version);
          q.setParameter("uri", uriInCaps);
        }
        List wtypes = q.getResultList();
        if (wtypes.size() == 0)
        {
          if (logger.isDebugEnabled()) {
            logger.debug("Zero object found for name" + name + ", returning null");
          }
          return null;
        }
        if (logger.isDebugEnabled()) {
          logger.debug("Loaded MetaObject by " + name);
        }
        mObject = wtypes;
        MetaInfo.MetaObject toBeReturned = (MetaInfo.MetaObject)mObject.get(0);
        if ((toBeReturned instanceof MetaInfo.Flow))
        {
          boolean isApp = checkIfFlowIsApplication(toBeReturned);
          if (isApp) {
            toBeReturned = convertFlowToApplication(toBeReturned);
          }
        }
        return toBeReturned;
      }
    }
    catch (Exception e)
    {
      if ((e instanceof MetaDataDBOpsException)) {
        throw new MetaDataDBOpsException(e.getMessage());
      }
      if ((e instanceof DatabaseException))
      {
        if ((((DatabaseException)e).getDatabaseErrorCode() == 40000) && ((((DatabaseException)e).getInternalException() instanceof SQLNonTransientConnectionException)))
        {
          logger.error("Shutting down server becasue it could not connect to database with error code: " + ((DatabaseException)e).getDatabaseErrorCode(), e);
          System.exit(1);
        }
      }
      else if ((e instanceof PersistenceException))
      {
        logger.error("Shutting down server becasue it could not connect to dataabase with error", e);
        System.exit(1);
      }
    }
    return null;
  }
  
  private Set<MetaInfo.MetaObject> loadByEntityType(EntityType eType)
    throws MetaDataDBOpsException
  {
    try
    {
      MDConstants.checkNullParams("Can't load Object by EntityType NULL", new Object[] { eType });
      if (eType.isStoreable())
      {
        List<MetaInfo.MetaObject> resultMetaObjects = null;
        if (logger.isDebugEnabled()) {
          logger.debug("Load by Entity Type : " + eType.toString());
        }
        EntityManager manager = getManager();
        synchronized (manager)
        {
          String query = "select w from MetaInfo$MetaObject w where w.type = :type";
          Query q = manager.createQuery(query);
          if (eType.ordinal() == EntityType.APPLICATION.ordinal()) {
            q.setParameter("type", EntityType.FLOW);
          } else {
            q.setParameter("type", eType);
          }
          List resultList = q.getResultList();
          if (resultList.size() == 0)
          {
            if (logger.isDebugEnabled()) {
              logger.debug("No Object of type : " + eType.toString() + " found in DB");
            }
            return null;
          }
          resultMetaObjects = resultList;
          if (eType.ordinal() == EntityType.APPLICATION.ordinal())
          {
            List<MetaInfo.MetaObject> appList = new ArrayList();
            for (MetaInfo.MetaObject appMetaObject : resultMetaObjects)
            {
              boolean isApp = checkIfFlowIsApplication(appMetaObject);
              if (isApp)
              {
                appMetaObject = convertFlowToApplication(appMetaObject);
                appList.add(appMetaObject);
              }
            }
            resultMetaObjects = appList;
          }
          else if (eType.ordinal() == EntityType.FLOW.ordinal())
          {
            List<MetaInfo.MetaObject> appList = new ArrayList();
            for (MetaInfo.MetaObject appMetaObject : resultMetaObjects)
            {
              boolean isApp = checkIfFlowIsApplication(appMetaObject);
              if (!isApp) {
                appList.add(appMetaObject);
              }
            }
            resultMetaObjects = appList;
          }
          return new HashSet(resultMetaObjects);
        }
      }
      return null;
    }
    catch (Exception e)
    {
      if ((e instanceof MetaDataDBOpsException)) {
        throw new MetaDataDBOpsException(e.getMessage());
      }
      if ((e instanceof DatabaseException))
      {
        if ((((DatabaseException)e).getDatabaseErrorCode() == 40000) && ((((DatabaseException)e).getInternalException() instanceof SQLNonTransientConnectionException)))
        {
          logger.error("Shutting down server becasue it could not connect to database with error code: " + ((DatabaseException)e).getDatabaseErrorCode(), e);
          System.exit(1);
        }
      }
      else if ((e instanceof PersistenceException))
      {
        logger.error("Shutting down server becasue it could not connect to dataabase with error", e);
        System.exit(1);
      }
    }
    return null;
  }
  
  private Set<MetaInfo.MetaObject> loadByNamespace(String namespace)
    throws MetaDataDBOpsException
  {
    try
    {
      MDConstants.checkNullParams("Can't load MetaObjects from NULL namespace", new Object[] { namespace });
      
      List<MetaInfo.MetaObject> resultMetaObjects = null;
      if (logger.isDebugEnabled()) {
        logger.debug("Load by Namespace : " + namespace);
      }
      EntityManager manager = getManager();
      synchronized (manager)
      {
        String query = "select w from MetaInfo$MetaObject w where w.nsName = :nsName";
        Query q = manager.createQuery(query);
        q.setParameter("nsName", namespace);
        
        List resultList = q.getResultList();
        if (resultList.size() == 0)
        {
          if (logger.isDebugEnabled()) {
            logger.debug("No Object of type : " + namespace + " found in DB");
          }
          return null;
        }
        resultMetaObjects = resultList;
        List<MetaInfo.MetaObject> toBeReturned = new ArrayList();
        for (MetaInfo.MetaObject metaObject : resultMetaObjects) {
          if (metaObject.getType().ordinal() == EntityType.FLOW.ordinal())
          {
            boolean isApp = checkIfFlowIsApplication(metaObject);
            if (isApp) {
              toBeReturned.add(convertFlowToApplication(metaObject));
            } else {
              toBeReturned.add(metaObject);
            }
          }
          else
          {
            toBeReturned.add(metaObject);
          }
        }
        return new HashSet(toBeReturned);
      }
    }
    catch (Exception e)
    {
      if ((e instanceof MetaDataDBOpsException)) {
        throw new MetaDataDBOpsException(e.getMessage());
      }
      if ((e instanceof DatabaseException))
      {
        if ((((DatabaseException)e).getDatabaseErrorCode() == 40000) && ((((DatabaseException)e).getInternalException() instanceof SQLNonTransientConnectionException)))
        {
          logger.error("Shutting down server becasue it could not connect to database with error code: " + ((DatabaseException)e).getDatabaseErrorCode(), e);
          System.exit(1);
        }
      }
      else if ((e instanceof PersistenceException))
      {
        logger.error("Shutting down server becasue it could not connect to dataabase with error", e);
        System.exit(1);
      }
    }
    return null;
  }
  
  private Set<MetaInfo.MetaObject> loadByEntityTypeInNamespace(String namespace, EntityType eType)
    throws MetaDataDBOpsException
  {
    try
    {
      MDConstants.checkNullParams("Can't load MetaObject with NULL values for either of, Entity Type: " + eType + ", Namespace: " + namespace, new Object[] { eType, namespace });
      if (eType.isStoreable())
      {
        List<MetaInfo.MetaObject> resultMetaObjects = null;
        if (logger.isDebugEnabled()) {
          logger.debug("Load by Namespace : " + namespace);
        }
        EntityManager manager = getManager();
        synchronized (manager)
        {
          String query = "select w from MetaInfo$MetaObject w where w.nsName = :nsName AND w.type = :type";
          Query q = manager.createQuery(query);
          q.setParameter("nsName", namespace);
          if (eType.ordinal() == EntityType.APPLICATION.ordinal()) {
            q.setParameter("type", EntityType.FLOW);
          } else {
            q.setParameter("type", eType);
          }
          List resultList = q.getResultList();
          if (resultList.size() == 0)
          {
            if (logger.isDebugEnabled()) {
              logger.debug("No Object of type : " + namespace + " found in DB");
            }
            return null;
          }
          resultMetaObjects = resultList;
          List<MetaInfo.MetaObject> toBeReturned = new ArrayList();
          if (eType.ordinal() == EntityType.APPLICATION.ordinal())
          {
            for (MetaInfo.MetaObject metaObject : resultMetaObjects)
            {
              boolean isApp = checkIfFlowIsApplication(metaObject);
              if (isApp) {
                toBeReturned.add(convertFlowToApplication(metaObject));
              }
            }
            resultMetaObjects = toBeReturned;
          }
          else if (eType.ordinal() == EntityType.FLOW.ordinal())
          {
            for (MetaInfo.MetaObject metaObject : resultMetaObjects)
            {
              boolean isApp = checkIfFlowIsApplication(metaObject);
              if (!isApp) {
                toBeReturned.add(metaObject);
              }
            }
            resultMetaObjects = toBeReturned;
          }
          return new HashSet(resultMetaObjects);
        }
      }
      return null;
    }
    catch (Exception e)
    {
      if ((e instanceof MetaDataDBOpsException)) {
        throw new MetaDataDBOpsException(e.getMessage());
      }
      if ((e instanceof DatabaseException))
      {
        if ((((DatabaseException)e).getDatabaseErrorCode() == 40000) && ((((DatabaseException)e).getInternalException() instanceof SQLNonTransientConnectionException)))
        {
          logger.error("Shutting down server becasue it could not connect to database with error code: " + ((DatabaseException)e).getDatabaseErrorCode(), e);
          System.exit(1);
        }
      }
      else if ((e instanceof PersistenceException))
      {
        logger.error("Shutting down server becasue it could not connect to dataabase with error", e);
        System.exit(1);
      }
    }
    return null;
  }
  
  public Integer loadMaxClassId()
  {
    EntityManager manager = getManager();
    synchronized (manager)
    {
      String query = "Select max(w.classId) from MetaInfo$Type w";
      Query q = manager.createQuery(query);
      Integer result = (Integer)q.getSingleResult();
      return result;
    }
  }
  
  private Integer removeByUUID(UUID uuid)
    throws MetaDataDBOpsException
  {
    EntityTransaction txn = null;
    Integer deleteCount = Integer.valueOf(0);
    try
    {
      MDConstants.checkNullParams("Can't remove Meta Object from Database with a NULL value UUID", new Object[] { uuid });
      
      String delQuery = "DELETE from MetaInfo$MetaObject w where w.uuid = :uuid";
      if (logger.isDebugEnabled()) {
        logger.debug("Deleted objects using query " + delQuery + " uuid = " + uuid);
      }
      EntityManager m = getManager();
      synchronized (m)
      {
        m.clear();
        
        txn = m.getTransaction();
        txn.begin();
        
        Query dq = m.createQuery(delQuery);
        dq.setParameter("uuid", uuid);
        
        deleteCount = Integer.valueOf(dq.executeUpdate());
        
        m.flush();
        
        txn.commit();
      }
    }
    catch (Exception e)
    {
      if ((txn != null) && (txn.isActive())) {
        txn.rollback();
      }
      logger.error("Problem deleting object with key " + uuid + " from Database \n" + "REASON: \n" + e.getMessage());
      if ((e instanceof DatabaseException))
      {
        if ((((DatabaseException)e).getDatabaseErrorCode() == 40000) && ((((DatabaseException)e).getInternalException() instanceof SQLNonTransientConnectionException)))
        {
          logger.error("Shutting down server becasue it could not connect to dataabase with error code: " + ((DatabaseException)e).getDatabaseErrorCode(), e);
          System.exit(1);
        }
      }
      else if ((e instanceof PersistenceException))
      {
        logger.error("Shutting down server becasue it could not connect to dataabase with error ", e);
        System.exit(1);
      }
    }
    return deleteCount;
  }
  
  private Integer removeByName(EntityType eType, String namespace, String name, Integer version)
    throws MetaDataDBOpsException
  {
    EntityTransaction txn = null;
    Integer deleteCount = Integer.valueOf(0);
    String uri = null;
    try
    {
      MDConstants.checkNullParams("Can't remove Meta Object from Database with a NULL for either of Entity Type, Namespace, Object Name, Version", new Object[] { eType, namespace, name, version });
      
      uri = MDConstants.makeURLWithVersion(eType, namespace, name, version);
      String delQuery = "DELETE from MetaInfo$MetaObject w where w.uri = :uri";
      if (logger.isDebugEnabled()) {
        logger.debug("Deleted objects using query " + delQuery + " uri = " + uri);
      }
      EntityManager m = getManager();
      synchronized (m)
      {
        m.clear();
        
        txn = m.getTransaction();
        txn.begin();
        
        Query dq = m.createQuery(delQuery);
        dq.setParameter("uri", uri);
        
        deleteCount = Integer.valueOf(dq.executeUpdate());
        
        m.flush();
        
        txn.commit();
      }
    }
    catch (Exception e)
    {
      if ((txn != null) && (txn.isActive())) {
        txn.rollback();
      }
      logger.error("Problem deleting object with key " + uri + " from store");
      if ((e instanceof DatabaseException))
      {
        if ((((DatabaseException)e).getDatabaseErrorCode() == 40000) && ((((DatabaseException)e).getInternalException() instanceof SQLNonTransientConnectionException)))
        {
          logger.error("Shutting down server becasue it could not connect to dataabase with error code: " + ((DatabaseException)e).getDatabaseErrorCode(), e);
          System.exit(1);
        }
      }
      else if ((e instanceof PersistenceException))
      {
        logger.error("Shutting down server becasue it could not connect to dataabase with error", e);
        System.exit(1);
      }
    }
    return deleteCount;
  }
  
  public Integer clear()
  {
    int n = 0;
    EntityTransaction txn = null;
    EntityManager em = null;
    try
    {
      em = getManager();
      synchronized (em)
      {
        String query = "Delete from MetaInfo$MetaObject w";
        if (logger.isInfoEnabled()) {
          logger.info("Deleting all rows from DB using " + query);
        }
        txn = em.getTransaction();
        txn.begin();
        TypedQuery<Object[]> q = em.createQuery(query, Object[].class);
        n = q.executeUpdate();
        logger.warn("total deleted : " + n);
        txn.commit();
      }
      return Integer.valueOf(n);
    }
    catch (Exception e)
    {
      logger.error("Problem deleting all from DB", e);
      if ((txn != null) && (txn.isActive())) {
        txn.rollback();
      }
      if ((e instanceof DatabaseException))
      {
        if ((((DatabaseException)e).getDatabaseErrorCode() == 40000) && ((((DatabaseException)e).getInternalException() instanceof SQLNonTransientConnectionException)))
        {
          logger.error("Shutting down server becasue it could not connect to database with error code: " + ((DatabaseException)e).getDatabaseErrorCode(), e);
          System.exit(1);
        }
      }
      else if ((e instanceof PersistenceException))
      {
        logger.error("Shutting down server becasue it could not connect to dataabase with error", e);
        System.exit(1);
      }
    }
    return Integer.valueOf(n);
  }
  
  public void shutdown()
  {
    EntityManager m = getManager();
    m.close();
  }
}
