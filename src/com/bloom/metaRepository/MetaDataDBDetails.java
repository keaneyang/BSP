package com.bloom.metaRepository;

import com.bloom.runtime.ConsoleReader;
import com.bloom.runtime.NodeStartUp;
import com.bloom.runtime.Server;
import com.bloom.runtime.NodeStartUp.InitializerParams.Params;
import com.bloom.security.WASecurityManager;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.security.GeneralSecurityException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.log4j.Logger;

public class MetaDataDBDetails
{
  String location = null;
  String name = null;
  String UName = null;
  String password = null;
  Connection conn = null;
  private static Logger logger = Logger.getLogger(MetaDataDBDetails.class);
  public final String default_db_port = "1527";
  public final String colon = ":";
  public final String default_db_location = HazelcastSingleton.getBindingInterface().concat(":").concat("1527");
  public static final String default_db_name = "wactionrepos";
  public static final String default_db_Uname = "waction";
  public static final String default_db_password = "w@ct10n";
  private boolean firstTime = true;
  
  public Map<String, String> takeDBDetails()
    throws IOException
  {
    boolean gotDBDetailsRight = false;
    while (!gotDBDetailsRight)
    {
      System.out.println("Required property \"Metadata Repository Location\" is undefined");
      gotDBDetailsRight = askDBDetails();
    }
    Map<String, String> dbDetails = new HashMap();
    dbDetails.put(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryLocation(), this.location);
    
    dbDetails.put(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryDBname(), this.name);
    
    dbDetails.put(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryUname(), this.UName);
    
    dbDetails.put(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryPass(), this.password);
    
    return dbDetails;
  }
  
  public static String encryptDBPassword(String toBeEncrypted, String saltString)
    throws GeneralSecurityException, UnsupportedEncodingException
  {
    Random r = new Random(saltString.hashCode());
    byte[] salt = new byte[8];
    r.nextBytes(salt);
    return WASecurityManager.encrypt(toBeEncrypted, salt);
  }
  
  public static String decryptDBPassword(String toBeDecrypted, String saltString)
    throws Exception
  {
    Random r = new Random(saltString.hashCode());
    byte[] salt = new byte[8];
    r.nextBytes(salt);
    return WASecurityManager.decrypt(toBeDecrypted, salt);
  }
  
  public boolean askDBDetails()
  {
    MetaDataDbProvider metaDataDbProvider = Server.getMetaDataDBProviderDetails();
    try
    {
      this.location = ConsoleReader.readLine(metaDataDbProvider.askMetaDataRepositoryLocation(this.default_db_location));
      
      this.name = "wactionrepos";
      this.UName = metaDataDbProvider.askDBUserName("waction");
      if (((metaDataDbProvider instanceof DerbyMD)) && (this.firstTime))
      {
        this.password = "w@ct10n";
        this.firstTime = false;
      }
      else
      {
        this.password = metaDataDbProvider.askDBPassword("w@ct10n");
      }
    }
    catch (IOException e)
    {
      logger.error("Could not get Metadata Repository Details beacause of IO problem, " + e.getMessage());
    }
    if ((this.location == null) || (this.location.isEmpty())) {
      this.location = metaDataDbProvider.askDefaultDBLocationAppendedWithPort(this.default_db_location);
    }
    if ((this.name == null) || (this.name.isEmpty())) {
      this.name = "wactionrepos";
    }
    if ((this.UName == null) || (this.UName.isEmpty())) {
      this.UName = "waction";
    }
    if ((this.password == null) || (this.password.isEmpty())) {
      this.password = "w@ct10n";
    }
    boolean b;
    try
    {
      b = createConnection(this.location, this.name, this.UName, this.password);
    }
    catch (SQLException e)
    {
      logger.error(e.getMessage(), e);
      b = false;
    }
    finally
    {
      closeConnection();
    }
    return b;
  }
  
  public boolean createConnection(String metaDataRepositoryLocation, String metaDataRepositoryName, String metaDataRepositoryUname, String metaDataRepositoryPass)
    throws SQLException
  {
    MetaDataDbProvider metaDataDbProvider = Server.getMetaDataDBProviderDetails();
    String dbURL = metaDataDbProvider.getJDBCURL(metaDataRepositoryLocation, metaDataRepositoryName, metaDataRepositoryUname, metaDataRepositoryPass);
    metaDataDbProvider.setJDBCDriver();
    boolean connected;
    try
    {
      this.conn = DriverManager.getConnection(dbURL, metaDataRepositoryUname, metaDataRepositoryPass);
      connected = true;
    }
    catch (Exception e)
    {
      throw e;
    }
    return connected;
  }
  
  public boolean isConnectionAlive(String metaDataRepositoryLocation, String metaDataRepositoryName, String metaDataRepositoryUname, String metaDataRepositoryPass)
  {
    boolean result = false;
    try
    {
      result = createConnection(metaDataRepositoryLocation, metaDataRepositoryName, metaDataRepositoryUname, metaDataRepositoryPass);
    }
    catch (SQLException e)
    {
      logger.error(e.getMessage(), e);
    }
    finally
    {
      closeConnection();
    }
    return result;
  }
  
  public void closeConnection()
  {
    if (this.conn != null) {
      try
      {
        this.conn.close();
        if (logger.isDebugEnabled()) {
          logger.debug("Test connection closed");
        }
      }
      catch (SQLException e)
      {
        logger.error("Connection not closed : " + e.getMessage(), e);
      }
    }
  }
}
