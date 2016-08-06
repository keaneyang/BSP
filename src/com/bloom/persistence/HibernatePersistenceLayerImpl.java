package com.bloom.persistence;

import com.bloom.gen.RTMappingGenerator;
import com.bloom.intf.PersistenceLayer;
import com.bloom.intf.PersistenceLayer.Range;
import com.bloom.runtime.NodeStartUp;
import com.bloom.waction.Waction;
import com.bloom.waction.WactionKey;
import com.bloom.recovery.Position;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.Dialect;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.ServiceRegistryBuilder;

public class HibernatePersistenceLayerImpl
  implements PersistenceLayer
{
  private static Logger logger = Logger.getLogger(HibernatePersistenceLayerImpl.class);
  private Map<String, Object> props = new HashMap();
  private String storeName = "default";
  private boolean metaDataRefreshed = false;
  private Configuration configure = null;
  private SessionFactory factory = null;
  private Session session = null;
  private static final String MXSQL_INSTR_PROP = "mxsql.tablecreation.instructions";
  
  public HibernatePersistenceLayerImpl(String persistenceUnitName)
  {
    this(persistenceUnitName, null);
  }
  
  public HibernatePersistenceLayerImpl(String pu, Map<String, Object> propss)
  {
    if (this.props != null)
    {
      this.props.putAll(propss);
      Set<String> keys = propss.keySet();
      for (String k : keys) {
        if (propss.get(k) == null) {
          this.props.remove(k);
        }
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("properties : " + this.props.toString());
    }
  }
  
  public void init()
  {
    init(null);
  }
  
  public void init(String storeName)
  {
    String mapping_file_name = NodeStartUp.getPlatformHome() + "/conf/" + storeName + ".hbm.xml";
    File file = new File(mapping_file_name);
    if (file.exists())
    {
      this.configure = new Configuration();
      this.configure.configure("hibernate.cfg.xml");
      this.configure.addFile(mapping_file_name);
      Properties properties = new Properties();
      properties.putAll(this.props);
      this.configure.setProperties(properties);
      ServiceRegistry serviceRegistry = new ServiceRegistryBuilder().applySettings(this.configure.getProperties()).buildServiceRegistry();
      this.factory = this.configure.buildSessionFactory(serviceRegistry);
    }
  }
  
  public int delete(Object object)
  {
    throw new NotImplementedException();
  }
  
  public PersistenceLayer.Range[] persist(Object object)
  {
    PersistenceLayer.Range[] rangeArray = new PersistenceLayer.Range[1];
    if (object == null)
    {
      logger.warn("Got null object to persist so, simply skip everything from here");
      return rangeArray;
    }
    if ((!this.metaDataRefreshed) && ((object instanceof Waction)))
    {
      String mapping_file_name = NodeStartUp.getPlatformHome() + "/conf/" + this.storeName + ".hbm.xml";
      String xml = RTMappingGenerator.getMappings(((Waction)object).getInternalWactionStoreName());
      if (logger.isDebugEnabled()) {
        logger.debug("hibernate xml mapping for wactionstore :\n" + xml);
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
        init(this.storeName);
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
      createTable();
      this.metaDataRefreshed = true;
    }
    synchronized (object)
    {
      if (logger.isTraceEnabled()) {
        logger.trace("persisting object of type : " + object.getClass().getCanonicalName() + "\nobject=" + object.toString());
      }
      try
      {
        if ((this.session == null) || (!this.session.isOpen())) {
          this.session = this.factory.openSession();
        }
        Transaction tx = this.session.beginTransaction();
        this.session.persist(object);
        tx.commit();
        if (logger.isTraceEnabled()) {
          logger.trace("done persisting object of type : " + object.getClass().getCanonicalName() + "\nobject=" + object.toString());
        }
      }
      finally {}
    }
    int numWritten = 1;
    if ((object instanceof List)) {
      numWritten = ((List)object).size();
    }
    rangeArray[0] = new PersistenceLayer.Range(0, numWritten);
    rangeArray[0].setSuccessful(true);
    return rangeArray;
  }
  
  private void createTable()
  {
    String dialectClassName = (String)this.props.get("hibernate.dialect");
    String inst = (String)this.props.get("mxsql.tablecreation.instructions");
    String hbm2ddl = (String)this.props.get("hibernate.hbm2ddl.auto");
    Connection conn = null;
    if ((hbm2ddl != null) && (hbm2ddl.equalsIgnoreCase("none")))
    {
      if (dialectClassName == null) {
        return;
      }
      try
      {
        Class dialectClazz = Class.forName(dialectClassName);
        Object dialect = (Dialect)dialectClazz.newInstance();
        dialectClazz.cast(dialect);
        String[] qrs = this.configure.generateSchemaCreationScript((Dialect)dialect);
        if ((inst == null) || (inst.trim().length() == 0)) {
          inst = "STORE BY PRIMARY KEY ATTRIBUTE BLOCKSIZE 4096";
        }
        String sql = qrs[0] + " " + inst + " ;";
        if (logger.isDebugEnabled()) {
          logger.debug("DDL to create table : \n" + sql);
        }
        String driver = (String)this.props.get("hibernate.connection.driver_class");
        String url = (String)this.props.get("hibernate.connection.url");
        String usr = (String)this.props.get("hibernate.connection.username");
        String pwd = (String)this.props.get("hibernate.connection.password");
        if (logger.isDebugEnabled()) {
          logger.debug("driver=" + driver + ", url=" + url + ", usr=" + usr + ",pwd=" + pwd);
        }
        Class.forName(driver);
        conn = DriverManager.getConnection(url, usr, pwd);
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(sql);
      }
      catch (SQLException|ClassNotFoundException|InstantiationException|IllegalAccessException ex)
      {
        logger.error("error in creating table :" + ex);
      }
      finally
      {
        try
        {
          conn.close();
        }
        catch (SQLException e) {}
      }
    }
    if (logger.isInfoEnabled()) {
      logger.info("Letting Hibernate to create table.");
    }
  }
  
  public void merge(Object object) {}
  
  public void setStoreName(String storeName, String tableName)
  {
    this.storeName = storeName;
  }
  
  public Object get(Class<?> objectClass, Object objectId)
  {
    return null;
  }
  
  public List<?> runQuery(String query, Map<String, Object> params, Integer maxResults)
  {
    return null;
  }
  
  public Object runNativeQuery(String query)
  {
    return null;
  }
  
  public int executeUpdate(String query, Map<String, Object> params)
  {
    return 0;
  }
  
  public <T extends Waction> List<T> getResults(Class<T> objectClass, Map<String, Object> filter, Set<WactionKey> excludeKeys)
  {
    String clazzName = objectClass.getCanonicalName();
    List<T> results = null;
    long T1 = System.currentTimeMillis();
    String qry = "FROM " + clazzName + " AS W ";
    if ((excludeKeys != null) && (excludeKeys.size() > 0))
    {
      qry = qry + " WHERE W.MapKeyString NOT IN ( ";
      Iterator<WactionKey> iter = excludeKeys.iterator();
      while (iter.hasNext())
      {
        WactionKey k = (WactionKey)iter.next();
        qry = qry + "'" + k.getWactionKeyStr() + "',";
      }
      qry = qry.substring(0, qry.length() - 1);
      qry = qry + ")";
    }
    long T2 = System.currentTimeMillis();
    if (logger.isDebugEnabled()) {
      logger.debug("SQL creation taken (milli sec) : " + (T2 - T1));
    }
    try
    {
      if ((this.session == null) || (!this.session.isOpen())) {
        this.session = this.factory.openSession();
      }
      Query query = this.session.createQuery(qry);
      results = query.list();
      List<T> localList1 = results;return localList1;
    }
    catch (Exception ex)
    {
      ex = ex;
      logger.error("error querying waction table :" + clazzName + ex);
    }
    finally {}
    return null;
  }
  
  public <T extends Waction> List<T> getResults(Class<T> objectClass, String wactionKey, Map<String, Object> filter)
  {
    String clazzName = objectClass.getCanonicalName();
    List<T> results = null;
    
    String qry = "FROM " + clazzName + "as W where W.MapKeyString = '" + wactionKey + "'";
    try
    {
      if ((this.session == null) || (!this.session.isOpen())) {
        this.session = this.factory.openSession();
      }
      Query query = this.session.createQuery(qry);
      results = query.list();
      List<T> localList1 = results;return localList1;
    }
    catch (Exception ex)
    {
      ex = ex;
      logger.error("error querying waction table :" + clazzName + ex);
    }
    finally {}
    return null;
  }
  
  public void close()
  {
    if ((this.session != null) && (this.session.isOpen()))
    {
      this.session.flush();
      this.session.close();
      this.factory.close();
    }
  }
  
  public Position getWSPosition(String namespaceName, String wactionStoreName)
  {
    throw new NotImplementedException("This method is not supported for HibernatePersistenceLayerImpl");
  }
  
  public boolean clearWSPosition(String namespaceName, String wactionStoreName)
  {
    throw new NotImplementedException("This method is not supported for HibernatePersistenceLayerImpl");
  }
  
  public WactionStore getWactionStore()
  {
    return null;
  }
  
  public void setWactionStore(WactionStore ws) {}
}
