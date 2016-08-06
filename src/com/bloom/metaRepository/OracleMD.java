package com.bloom.metaRepository;

import java.io.IOException;
import java.math.BigDecimal;
import org.apache.log4j.Logger;

import com.bloom.runtime.ConsoleReader;
import com.bloom.runtime.components.EntityType;

public class OracleMD
  implements MetaDataDbProvider
{
  private static Logger logger = Logger.getLogger(OracleMD.class);
  
  public String getJDBCDriver()
  {
    return "oracle.jdbc.OracleDriver";
  }
  
  public Object getIntegerValueFromData(Object dataValue)
  {
    try
    {
      assert ((dataValue instanceof BigDecimal));
      Integer data = Integer.valueOf(((BigDecimal)dataValue).intValueExact());
      return EntityType.forObject(data);
    }
    catch (AssertionError e)
    {
      logger.error("Expecting Big Decimal instance type, but received some other type" + e);
      System.exit(1);
    }
    return null;
  }
  
  public String askMetaDataRepositoryLocation(String default_db_location)
  {
    return "Enter Metadata Repository URL: ";
  }
  
  public String askDefaultDBLocationAppendedWithPort(String default_db_location)
  {
    String[] ip_address = default_db_location.split(":");
    String port = "1521";
    String service = "orcl";
    return ip_address[0] + ":" + port + ":" + service;
  }
  
  public String askDBUserName(String default_db_Uname)
  {
    try
    {
      return ConsoleReader.readLine("Enter DB User name [default waction] ( Press Enter/Return to default) : ");
    }
    catch (IOException e)
    {
      logger.error("Could not get Metadata Repository user details beacause of IO problem, " + e.getMessage());
    }
    return null;
  }
  
  public String askDBPassword(String default_db_password)
  {
    try
    {
      return ConsoleReader.readPassword("Enter DB password [default waction]( Press Enter/Return to default) : ");
    }
    catch (IOException e)
    {
      logger.error("Could not get Metadata Repository password beacause of IO problem, " + e.getMessage());
    }
    return null;
  }
  
  public void setJDBCDriver()
  {
    try
    {
      Class.forName("oracle.jdbc.driver.OracleDriver").newInstance();
    }
    catch (ClassNotFoundException e)
    {
      logger.error("Oracle JDBC jar may be missing; if missing add it to Bloom/libs/ path and restart the server: " + e);
    }
    catch (InstantiationException e)
    {
      logger.error("JDBC driver instantiation failed, please check: " + e);
    }
    catch (IllegalAccessException e)
    {
      logger.error("Currently executing method does not have access to instantiate the jdbc driver: " + e);
    }
  }
  
  public String getJDBCURL(String metaDataRepositoryLocation, String metaDataRepositoryName, String metaDataRepositoryUname, String metaDataRepositoryPass)
  {
    return metaDataRepositoryLocation;
  }
}
