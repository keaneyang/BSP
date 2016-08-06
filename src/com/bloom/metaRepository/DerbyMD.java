package com.bloom.metaRepository;

import java.io.IOException;
import org.apache.log4j.Logger;

import com.bloom.runtime.ConsoleReader;
import com.bloom.runtime.components.EntityType;

public class DerbyMD
  implements MetaDataDbProvider
{
  private static Logger logger = Logger.getLogger(DerbyMD.class);
  
  public String getJDBCDriver()
  {
    return "org.apache.derby.jdbc.ClientDriver";
  }
  
  public Object getIntegerValueFromData(Object dataValue)
  {
    return EntityType.forObject(dataValue);
  }
  
  public String askMetaDataRepositoryLocation(String default_db_location)
  {
    return "Enter Metadata Repository Location [Format = IPAddress:port] [default " + default_db_location + " (Press Enter/Return to default)] : ";
  }
  
  public String askDefaultDBLocationAppendedWithPort(String default_db_location)
  {
    return default_db_location;
  }
  
  public String askDBUserName(String default_db_Uname)
  {
    return default_db_Uname;
  }
  
  public String askDBPassword(String default_db_password)
  {
    try
    {
      String db_password = ConsoleReader.readPassword("Enter DB password: ");
      return db_password.isEmpty() ? default_db_password : db_password;
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
      Class.forName("org.apache.derby.jdbc.ClientDriver").newInstance();
    }
    catch (InstantiationException|IllegalAccessException|ClassNotFoundException e)
    {
      logger.warn("Unable to create db connection : " + e.getMessage());
    }
  }
  
  public String getJDBCURL(String metaDataRepositoryLocation, String metaDataRepositoryName, String metaDataRepositoryUname, String metaDataRepositoryPass)
  {
    return "jdbc:derby://" + metaDataRepositoryLocation + "/" + metaDataRepositoryName;
  }
}
