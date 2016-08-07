package com.bloom.runtime;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import javax.persistence.EntityManager;
import javax.persistence.Query;
import org.apache.log4j.Logger;

import com.bloom.metaRepository.MetaDataDBOps;

public final class Version
{
  private static Logger logger = Logger.getLogger(Version.class);
  private static final String BUILD_PROPERTIES = "build.properties";
  public static final String NOT_APPLICABLE = "n/a";
  
  private static String getPropertyValue(InputStream inStream, String propertyName, String defaultValue)
  {
    String value = defaultValue;
    Properties properties = new Properties();
    if (inStream != null) {
      try
      {
        properties.load(inStream);
        value = properties.getProperty(propertyName, value);
      }
      catch (IOException e)
      {
        Logger logger = Logger.getLogger(Version.class);
        logger.error(e);
      }
    }
    return value;
  }
  
  private static String getPropertyValue(String propertyFile, String propertyName, String defaultValue)
  {
    InputStream inStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(propertyFile);
    return getPropertyValue(inStream, propertyName, defaultValue);
  }
  
  public static String getPOMVersionString()
  {
    return getPropertyValue("build.properties", "pom_version", "1.0.0-SNAPSHOT");
  }
  
  private static String getBuildRevisionString()
  {
    return getPropertyValue("build.properties", "scm_revision", "Development Version - Not for Release");
  }
  
  public static String getVersionString()
  {
    return "Version " + getPOMVersionString() + " (" + getBuildRevisionString() + ")";
  }
  
  private static String mdVersion = null;
  
  public static String getMetaDataVersion()
  {
    if (mdVersion == null)
    {
      EntityManager entityManager = MetaDataDBOps.getManager();
      String qry = "select version from ".concat(MetaDataDBOps.DBUname).concat(".").concat("bloom_version");
      try
      {
        Query query = entityManager.createNativeQuery(qry);
        mdVersion = (String)query.getSingleResult();
      }
      catch (Exception ex)
      {
        ex = 
        
          ex;logger.error("error executing " + qry, ex);
      }
      finally {}
      if (logger.isInfoEnabled()) {
        logger.info("MetaData version : " + mdVersion);
      }
    }
    return mdVersion;
  }
  
  public static boolean isConnectedToCompatibleMetaData()
  {
    boolean result = false;
    String mdv = getMetaDataVersion();
    String codeVersion = getPOMVersionString();
    if (logger.isDebugEnabled()) {
      logger.debug("codeVersion:" + codeVersion + ", md version:" + mdv);
    }
    if (codeVersion.equals("1.0.0-SNAPSHOT")) {
      return true;
    }
    if ((mdv == null) || (mdv.isEmpty())) {
      return result;
    }
    String[] parts1 = mdv.split("\\.");
    String major = "";
    String minor = "";
    String patch = "";
    if (parts1.length > 1)
    {
      major = parts1[0];
      minor = parts1[1];
    }
    else
    {
      return result;
    }
    String majorc = "";
    String minorc = "";
    String patchc = "";
    String[] parts = codeVersion.split("\\.");
    if (parts.length > 1)
    {
      majorc = parts[0];
      minorc = parts[1];
    }
    else
    {
      return result;
    }
    if ((majorc.equalsIgnoreCase(major)) && (
      (minorc.equalsIgnoreCase(minor)) || (Integer.parseInt(minorc) > Integer.parseInt(minor)))) {
      result = true;
    }
    return result;
  }
  
  public static String getBuildPropFileName()
  {
    return "build.properties";
  }
}
