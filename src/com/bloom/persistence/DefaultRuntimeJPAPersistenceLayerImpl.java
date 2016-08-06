package com.bloom.persistence;

import com.bloom.gen.RTMappingGenerator;
import com.bloom.gen.WAMetadataSource;
import com.bloom.runtime.NodeStartUp;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;

public class DefaultRuntimeJPAPersistenceLayerImpl
  extends DefaultJPAPersistenceLayerImpl
{
  private static Logger logger = Logger.getLogger(DefaultRuntimeJPAPersistenceLayerImpl.class);
  
  public DefaultRuntimeJPAPersistenceLayerImpl() {}
  
  public DefaultRuntimeJPAPersistenceLayerImpl(String persistenceUnit, Map<String, Object> properties)
  {
    super(persistenceUnit, properties);
  }
  
  protected void createPersistenceFile()
  {
    String pxml = "";
    try
    {
      String persistence_file_name = "persistence_sample.xml";
      InputStream in = ClassLoader.getSystemResourceAsStream(persistence_file_name);
      char[] buffer = new char[1024];
      StringBuilder sb = new StringBuilder();
      Reader reader = new InputStreamReader(in);
      int nn = 0;
      for (;;)
      {
        nn = reader.read(buffer);
        if (nn < 0) {
          break;
        }
        sb.append(buffer, 0, nn);
      }
      pxml = sb.toString();
      pxml = pxml.replace("$PUNAME", this.persistenceUnitName);
      if (logger.isDebugEnabled()) {
        logger.debug("persistence xml for store : " + this.persistenceUnitName + "\n" + pxml);
      }
    }
    catch (IOException ex)
    {
      logger.error("error creating custom persistence.xml file for :" + this.persistenceUnitName + ex);
    }
    finally {}
    synchronized (WactionStore.class)
    {
      File metadir = new File(NodeStartUp.getPlatformHome() + "/conf/META-INF");
      if (!metadir.exists()) {
        metadir.mkdir();
      }
    }
    String persistence_file_name = NodeStartUp.getPlatformHome() + "/conf/META-INF/persistence_" + this.persistenceUnitName + ".xml";
    BufferedWriter writer = null;
    try
    {
      writer = new BufferedWriter(new FileWriter(new File(persistence_file_name)));
      writer.write(pxml);
      try
      {
        if (writer != null) {
          writer.close();
        }
      }
      catch (IOException ie) {}
      if (!logger.isDebugEnabled()) {
        return;
      }
    }
    catch (Exception e)
    {
      logger.error("error writing persistence file ", e);
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
    logger.debug("successfully created persistence xml file for persistence unit :" + this.persistenceUnitName);
  }
  
  protected void createORMXMLMappingFile()
  {
    String mapping_file_name = NodeStartUp.getPlatformHome() + "/conf/eclipselink-orm-" + this.persistenceUnitName + ".xml";
    String xml = RTMappingGenerator.getMappings(this.persistenceUnitName);
    if (logger.isDebugEnabled()) {
      logger.debug("xml mapping for event :\n" + xml);
    }
    BufferedWriter writer = null;
    try
    {
      writer = new BufferedWriter(new FileWriter(new File(mapping_file_name)));
      writer.write(xml); return;
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
  
  public void init()
  {
    if ((this.factory == null) || (!this.factory.isOpen()))
    {
      createPersistenceFile();
      createORMXMLMappingFile();
      
      this.props.put("eclipselink.persistencexml", "META-INF/persistence_" + this.persistenceUnitName + ".xml");
      WAMetadataSource mms = new WAMetadataSource();
      mms.setStoreName(this.persistenceUnitName);
      this.props.put("eclipselink.metadata-source", mms);
      this.factory = Persistence.createEntityManagerFactory(this.persistenceUnitName, this.props);
      Map<String, Object> origMap = this.factory.getProperties();
      for (String key : origMap.keySet()) {
        if (!this.props.containsKey(key)) {
          this.props.put(key.toUpperCase(), origMap.get(key));
        }
      }
      EntityManager em = getEntityManager();
      if (em != null)
      {
        em.close();
        em = null;
      }
    }
  }
  
  public int delete(Object object)
  {
    throw new NotImplementedException();
  }
}
