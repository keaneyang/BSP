package com.bloom.runtime;

import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.metaRepository.MetaDataDBDetails;
import com.bloom.metaRepository.MetaDataDbProvider;
import com.bloom.runtime.exceptions.ErrorCode;
import com.bloom.runtime.exceptions.ErrorCode.Error;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Initializer;
import com.bloom.security.WASecurityManager;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.bloom.license.LicenseManager;
import com.bloom.license.LicenseManager.Option;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.security.GeneralSecurityException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import org.apache.log4j.Logger;

public class NodeStartUp
{
  private static Logger logger = Logger.getLogger(NodeStartUp.class);
  private boolean startUpFileExists = false;
  private MetaInfo.Initializer initializer = null;
  private String bindingInterface = null;
  public static final String startup_file_name = "startUp.properties";
  public static final String ConfigDirName = "conf";
  private boolean nodeStarted = false;
  private static String platformHome = null;
  private final String from_constant_string = "com.bloom.platform.from";
  
  public NodeStartUp(boolean runningWithDB)
  {
    startUp(runningWithDB);
  }
  
  public static String encrypt(String password, String clusterName)
  {
    String returnPass = null;
    try
    {
      Random r = new Random(clusterName.hashCode());
      byte[] salt = new byte[8];
      r.nextBytes(salt);
      returnPass = WASecurityManager.encrypt(password, salt);
    }
    catch (UnsupportedEncodingException|GeneralSecurityException e)
    {
      logger.error(e.getLocalizedMessage());
    }
    return returnPass;
  }
  
  public MetaInfo.Initializer getInitializer()
  {
    return this.initializer;
  }
  
  public void startUp(boolean runningWithDB)
  {
    setPlatformHome();
    Properties props = readPropertiesFile();
    if (this.startUpFileExists)
    {
      List<ErrorCode.Error> e = null;
      if (props != null) {
        e = checkFormat(props);
      } else {
        logger.error("Properties were loaded from file but props map was still null.");
      }
      if ((e == null) || (e.isEmpty()))
      {
        startingUpWithStartUpFile(props, runningWithDB);
      }
      else
      {
        handleErrors(props, e);
        writePropertiesToFile(props);
        startingUpWithStartUpFile(props, runningWithDB);
      }
    }
    else
    {
      try
      {
        Properties lprop = new Properties();
        Map<String, String> clusterDetails = takeClusterDetails();
        lprop.putAll(clusterDetails);
        
        Map<String, String> licenseKeyDetails = takeLicenseKeyInfo(lprop);
        clusterDetails.putAll(licenseKeyDetails);
        if (!clusterDetails.isEmpty())
        {
          HazelcastInstance instance = HazelcastSingleton.get((String)clusterDetails.get(NodeStartUp.InitializerParams.Params.getWAClusterName()));
          if (instance != null)
          {
            this.bindingInterface = HazelcastSingleton.getBindingInterface();
            
            IMap<String, MetaInfo.Initializer> startUpMap = instance.getMap("#startUpMap");
            if (logger.isDebugEnabled()) {
              logger.debug("begin start up map : " + startUpMap.entrySet());
            }
            String cName = (String)clusterDetails.get(NodeStartUp.InitializerParams.Params.getWAClusterName());
            
            startUpMap.lock(cName);
            System.out.println("Starting Server on cluster : " + cName);
            if (startUpMap.containsKey(cName)) {
              noStartUpFileExistsAndObjectInMemory(cName, clusterDetails, runningWithDB, startUpMap);
            } else {
              noStartUpFileExistsAndObjectNotInMemory(clusterDetails, runningWithDB, startUpMap);
            }
            startUpMap.unlock(cName);
            if (logger.isInfoEnabled()) {
              logger.info("Node started up(startUp File does not Exist)");
            }
            setNodeStarted(true);
          }
          else
          {
            logger.error("Hazelcast Instance was null");
          }
        }
        else
        {
          logger.error("Cluster details were not entered correctly and failed 3 times, please restart the system");
        }
      }
      catch (IOException e)
      {
        logger.error("Input reader failed!!");
      }
      catch (RuntimeException e)
      {
        System.err.println(ErrorCode.Error.SERVERCANNOTSTART);
        System.err.println("Reason : " + e.getMessage());
      }
    }
  }
  
  private void handleErrors(Properties props, List<ErrorCode.Error> e)
  {
    boolean justTookClusterDetails = false;
    boolean justTookDBDetails = false;
    for (ErrorCode.Error ee : e) {
      if (ee.equals(ErrorCode.Error.CLUSTERNAME))
      {
        System.err.println(ee);
        Map<String, String> clusterDetails = takeClusterDetails();
        justTookClusterDetails = true;
        props.putAll(clusterDetails);
      }
      else if ((ee.equals(ErrorCode.Error.CLUSTERPASSWORD)) && (!justTookClusterDetails))
      {
        System.err.println(ee);
        String pass = takePassword((String)props.get(NodeStartUp.InitializerParams.Params.getWAClusterName()));
        
        props.put(NodeStartUp.InitializerParams.Params.getWAClusterPassword(), pass);
      }
      else if (ee.equals(ErrorCode.Error.INTERFACES))
      {
        System.err.println(ee);
        String ss = HazelcastSingleton.chooseInterface(HazelcastSingleton.getInterface());
        if (logger.isInfoEnabled()) {
          logger.info("New interface after it got erased from file : " + ss);
        }
        props.put(NodeStartUp.InitializerParams.Params.getInterfaces(), ss);
      }
      else if (((ee.equals(ErrorCode.Error.DATABASELOCATION)) || (ee.equals(ErrorCode.Error.DATABASENAME)) || (ee.equals(ErrorCode.Error.DATABASEUNAME)) || (ee.equals(ErrorCode.Error.DATABASEPASS))) && (!justTookDBDetails))
      {
        System.err.println(ee);
        
        HazelcastSingleton.get(props.getProperty(NodeStartUp.InitializerParams.Params.getWAClusterName()));
        
        MetaDataDBDetails dbChecking = new MetaDataDBDetails();
        try
        {
          Map<String, String> dbDetails = dbChecking.takeDBDetails();
          
          props.putAll(dbDetails);
          justTookDBDetails = true;
        }
        catch (IOException e1)
        {
          logger.error(e1.getMessage());
        }
      }
      else if ((ee.equals(ErrorCode.Error.NOLICENSEKEYSET)) || (ee.equals(ErrorCode.Error.NOPRODUCTKEYSET)) || (ee.equals(ErrorCode.Error.NOCOMPANYNAMESET)))
      {
        System.err.println(ee);
        try
        {
          Map<String, String> licenseKeyDetails = takeLicenseKeyInfo(props);
          props.putAll(licenseKeyDetails);
        }
        catch (IOException e1)
        {
          logger.error(e1.getMessage());
        }
      }
    }
  }
  
  private static List<ErrorCode.Error> checkFormat(Properties props)
  {
    List<ErrorCode.Error> returnValue = new ArrayList();
    String ss = null;
    
    ss = (String)props.get(NodeStartUp.InitializerParams.Params.getWAClusterName());
    if ((ss == null) || (ss.isEmpty()) || (ss.equalsIgnoreCase("null"))) {
      returnValue.add(ErrorCode.Error.CLUSTERNAME);
    }
    ss = (String)props.get(NodeStartUp.InitializerParams.Params.getWAClusterPassword());
    if ((ss == null) || (ss.isEmpty()) || (ss.equalsIgnoreCase("null"))) {
      returnValue.add(ErrorCode.Error.CLUSTERPASSWORD);
    }
    ss = (String)props.get(NodeStartUp.InitializerParams.Params.getInterfaces());
    if ((ss == null) || (ss.isEmpty()) || (ss.equalsIgnoreCase("null"))) {
      returnValue.add(ErrorCode.Error.INTERFACES);
    }
    ss = (String)props.get(NodeStartUp.InitializerParams.Params.getLicenceKey());
    if ((ss == null) || (ss.isEmpty()) || (ss.equalsIgnoreCase("null"))) {
      returnValue.add(ErrorCode.Error.NOLICENSEKEYSET);
    }
    ss = (String)props.get(NodeStartUp.InitializerParams.Params.getProductKey());
    if ((ss == null) || (ss.isEmpty()) || (ss.equalsIgnoreCase("null"))) {
      returnValue.add(ErrorCode.Error.NOPRODUCTKEYSET);
    }
    ss = (String)props.get(NodeStartUp.InitializerParams.Params.getCompanyName());
    if ((ss == null) || (ss.isEmpty()) || (ss.equalsIgnoreCase("null"))) {
      returnValue.add(ErrorCode.Error.NOCOMPANYNAMESET);
    }
    String persist = System.getProperty("com.bloom.config.persist");
    if (logger.isDebugEnabled()) {
      logger.debug("persist :" + persist);
    }
    if ((persist != null) && (persist.equalsIgnoreCase("True")))
    {
      ss = (String)props.get(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryLocation());
      if ((ss == null) || (ss.isEmpty()) || (ss.equalsIgnoreCase("null"))) {
        returnValue.add(ErrorCode.Error.DATABASELOCATION);
      }
      ss = (String)props.get(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryDBname());
      if ((ss == null) || (ss.isEmpty()) || (ss.equalsIgnoreCase("null"))) {
        returnValue.add(ErrorCode.Error.DATABASENAME);
      }
      ss = (String)props.get(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryUname());
      if ((ss == null) || (ss.isEmpty()) || (ss.equalsIgnoreCase("null"))) {
        returnValue.add(ErrorCode.Error.DATABASEUNAME);
      }
      ss = (String)props.get(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryPass());
      if ((ss == null) || (ss.isEmpty()) || (ss.equalsIgnoreCase("null"))) {
        returnValue.add(ErrorCode.Error.DATABASEPASS);
      }
    }
    return returnValue;
  }
  
  private Collection<String> parseInterfacesParam(String interfaces)
  {
    Set<String> ret = new HashSet();
    for (String s : interfaces.split(",")) {
      ret.add(s);
    }
    return ret;
  }
  
  private void startingUpWithStartUpFile(Properties props, boolean runningWithDB)
  {
    if (logger.isInfoEnabled()) {
      logger.info("Starting with file");
    }
    String cName = (String)props.get(NodeStartUp.InitializerParams.Params.getWAClusterName());
    
    System.out.println("Starting Server on cluster : " + cName);
    if (logger.isInfoEnabled()) {
      logger.info("Will search for initializer object with name: " + cName + " in memory.");
    }
    String iniParams = (String)props.get(NodeStartUp.InitializerParams.Params.getInterfaces());
    if (logger.isInfoEnabled()) {
      logger.info("Interfaces found in startup file : " + iniParams);
    }
    System.out.println("Interfaces found in startup file : " + parseInterfacesParam(iniParams));
    
    HazelcastInstance instance = null;
    try
    {
      instance = HazelcastSingleton.get(cName, iniParams);
    }
    catch (Exception e)
    {
      System.err.println(ErrorCode.Error.SERVERCANNOTSTART);
      System.err.println("Reason : " + e.getMessage());
    }
    this.bindingInterface = HazelcastSingleton.getBindingInterface();
    boolean writeOutInterfaces = HazelcastSingleton.passedInterfacesDidNotMatchAvailableInterfaces;
    if (instance != null)
    {
      IMap<String, MetaInfo.Initializer> startUpMap = instance.getMap("#startUpMap");
      if (logger.isInfoEnabled()) {
        logger.info("begin start up map : " + startUpMap.entrySet());
      }
      if (startUpMap.containsKey(cName)) {
        startUpFileExistsAndObjectInMemory(cName, props, startUpMap, runningWithDB, writeOutInterfaces);
      } else {
        startUpFileExistsAndObjectNotInMemory(cName, props, startUpMap, runningWithDB, writeOutInterfaces);
      }
      if (logger.isInfoEnabled()) {
        logger.info("Node started up(startUp File Exists)");
      }
      setNodeStarted(true);
    }
  }
  
  public static String getPlatformHome()
  {
    if (logger.isTraceEnabled()) {
      logger.trace("Getting platform home");
    }
    return platformHome;
  }
  
  public static void setPlatformHome()
  {
    String home_dir = System.getProperty("com.bloom.platform.home");
    if ((home_dir != null) && (home_dir.contains(";")))
    {
      String[] home_dir_list = home_dir.split(";");
      for (int i = 0; i < home_dir_list.length; i++) {
        if (home_dir_list[i].contains("Platform"))
        {
          platformHome = home_dir_list[i];
          break;
        }
      }
    }
    else
    {
      platformHome = home_dir;
    }
    if (platformHome == null) {
      platformHome = ".";
    }
  }
  
  private Properties readPropertiesFile()
  {
    String filePath = getPlatformHome().concat("/").concat("conf").concat("/").concat("startUp.properties");
    File f = new File(filePath);
    Properties props = new Properties();
    if (f.exists()) {
      try
      {
        props.load(new FileInputStream(f));
        this.startUpFileExists = true;
      }
      catch (IOException|NullPointerException e)
      {
        logger.error("Error while reading extant start-up file.", e);
      }
    }
    if ((props != null) && (!props.isEmpty()))
    {
      String propsClusterName = (String)props.get(NodeStartUp.InitializerParams.Params.getWAClusterName());
      if (propsClusterName != null)
      {
        String argClusterName = System.getProperty("com.bloom.config.clusterName");
        if ((argClusterName != null) && (!argClusterName.isEmpty()) && (!propsClusterName.equals(argClusterName)))
        {
          logger.error("Cannot start cluster with name " + argClusterName + " when expecting name " + propsClusterName + ". Please use '-c " + propsClusterName + "' or clear startup properties file.");
          System.exit(1);
        }
      }
      String dbPassword = (String)props.get(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryPass());
      if (dbPassword == null)
      {
        dbPassword = Server.metaDataDbProvider.askDBPassword("w@ct10n");
        props.put(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryPass(), dbPassword);
        Properties props_copy = new Properties();
        props_copy.putAll(props);
        writePropertiesToFile(props_copy);
      }
      else
      {
        try
        {
          if ((ServerUpgradeUtility.isUpgrading) && (isVersionBefore321()))
          {
            if (isCurrentVersionGreaterThanEqualTo321())
            {
              Properties props_copy = new Properties();
              props_copy.putAll(props);
              writePropertiesToFile(props_copy);
            }
          }
          else
          {
            String dbPassword_decrypted = MetaDataDBDetails.decryptDBPassword(dbPassword, "wactionrepos");
            props.put(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryPass(), dbPassword_decrypted);
          }
        }
        catch (Exception e)
        {
          logger.error("Metadata password decryption failed, hence shutting down Server: " + e.getMessage() + ". In Server upgrade routine: " + ServerUpgradeUtility.isUpgrading);
          System.exit(1);
        }
      }
    }
    return props;
  }
  
  private boolean isVersionBefore321()
  {
    String from = System.getProperty("com.bloom.platform.from");
    if ((from == null) || (from.isEmpty())) {
      return false;
    }
    ReleaseNumber from_release_number = ReleaseNumber.createReleaseNumber(from);
    ReleaseNumber base_release_number = ReleaseNumber.createReleaseNumber(3, 2, 1);
    return from_release_number.lessThan(base_release_number);
  }
  
  private boolean isCurrentVersionGreaterThanEqualTo321()
  {
    String current_version_string = Version.getPOMVersionString();
    ReleaseNumber current_release_number = ReleaseNumber.createReleaseNumber(current_version_string);
    if (current_release_number.isDevVersion()) {
      return true;
    }
    ReleaseNumber base_release_number = ReleaseNumber.createReleaseNumber(3, 2, 1);
    return current_release_number.greaterThanEqualTo(base_release_number);
  }
  
  private synchronized Properties writePropertiesToFile(Properties p)
  {
    String dbPassword = (String)p.get(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryPass());
    String dbPassword_encrypted = null;
    try
    {
      dbPassword_encrypted = MetaDataDBDetails.encryptDBPassword(dbPassword, "wactionrepos");
    }
    catch (GeneralSecurityException e)
    {
      logger.error("Metadata password encryption failed, hence shutting down Server", e);
      System.exit(1);
    }
    catch (UnsupportedEncodingException e)
    {
      logger.error("Metadata password encryption failed, hence shutting down Server", e);
      System.exit(1);
    }
    p.put(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryPass(), dbPassword_encrypted);
    File propertyFile = new File(getPlatformHome().concat("/").concat("conf").concat("/").concat("startUp.properties"));
    if (propertyFile.exists()) {
      propertyFile.delete();
    }
    if (logger.isInfoEnabled()) {
      logger.info("Location : " + propertyFile.getAbsolutePath());
    }
    try
    {
      FileOutputStream fos = new FileOutputStream(propertyFile);
      p.store(fos, "Initial Setup");
      fos.close();
    }
    catch (IOException e)
    {
      logger.error(e.getMessage());
    }
    return p;
  }
  
  public void startUpFileExistsAndObjectInMemory(String cName, Properties tempProps, IMap<String, MetaInfo.Initializer> startUpMap, boolean runningWithDB, boolean writeOutInterfaces)
  {
    if (logger.isInfoEnabled()) {
      logger.info("In-memory object available");
    }
    startUpMap.lock(cName);
    this.initializer = ((MetaInfo.Initializer)startUpMap.get(cName));
    this.initializer.currentData();
    boolean dataInFileIsIncorrect = false;
    Properties properties = new Properties();
    if (runningWithDB) {
      if ((!tempProps.get(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryLocation()).equals(this.initializer.MetaDataRepositoryLocation)) || (!tempProps.get(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryDBname()).equals(this.initializer.MetaDataRepositoryDBname)) || (!tempProps.get(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryUname()).equals(this.initializer.MetaDataRepositoryUname)) || (!tempProps.get(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryPass()).equals(this.initializer.MetaDataRepositoryPass)))
      {
        dataInFileIsIncorrect = true;
        
        properties.put(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryLocation(), this.initializer.MetaDataRepositoryLocation);
        
        properties.put(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryDBname(), this.initializer.MetaDataRepositoryDBname);
        
        properties.put(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryUname(), this.initializer.MetaDataRepositoryUname);
        
        properties.put(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryPass(), this.initializer.MetaDataRepositoryPass);
      }
    }
    String tempPassword = (String)tempProps.get(NodeStartUp.InitializerParams.Params.getWAClusterPassword());
    
    String lKEY = (String)tempProps.get(NodeStartUp.InitializerParams.Params.getLicenceKey());
    
    String pKEY = (String)tempProps.get(NodeStartUp.InitializerParams.Params.getProductKey());
    
    String compName = (String)tempProps.get(NodeStartUp.InitializerParams.Params.getCompanyName());
    
    boolean authorized = this.initializer.authenticate(tempPassword);
    if (!authorized) {
      dataInFileIsIncorrect = true;
    }
    while (!authorized)
    {
      logger.error("Password in file did not match the in-memory copy");
      tempPassword = null;
      tempPassword = takePassword(cName);
      authorized = this.initializer.authenticate(tempPassword);
    }
    if (logger.isInfoEnabled()) {
      logger.info("Verified");
    }
    if (logger.isInfoEnabled()) {
      logger.info("Writing out new Interface");
    }
    String intfs = (String)tempProps.get(NodeStartUp.InitializerParams.Params.getInterfaces());
    if (intfs != null)
    {
      if (intfs.isEmpty())
      {
        intfs = this.bindingInterface;
        dataInFileIsIncorrect = true;
      }
      else if (!intfs.contains(this.bindingInterface))
      {
        intfs = intfs + "," + this.bindingInterface;
        dataInFileIsIncorrect = true;
      }
    }
    else
    {
      intfs = this.bindingInterface;
      dataInFileIsIncorrect = true;
    }
    if (dataInFileIsIncorrect)
    {
      properties.put(NodeStartUp.InitializerParams.Params.getInterfaces(), intfs);
      properties.put(NodeStartUp.InitializerParams.Params.getWAClusterName(), cName);
      properties.put(NodeStartUp.InitializerParams.Params.getWAClusterPassword(), tempPassword);
      
      properties.put(NodeStartUp.InitializerParams.Params.getLicenceKey(), lKEY);
      properties.put(NodeStartUp.InitializerParams.Params.getProductKey(), pKEY);
      properties.put(NodeStartUp.InitializerParams.Params.getCompanyName(), compName);
    }
    if (!properties.isEmpty()) {
      writePropertiesToFile(properties);
    } else if (logger.isInfoEnabled()) {
      logger.info("@startUpFileExistsAndObjectInMemory : No new properties are created.");
    }
    startUpMap.unlock(cName);
  }
  
  public void startUpFileExistsAndObjectNotInMemory(String cName, Properties tempProps, IMap<String, MetaInfo.Initializer> startUpMap, boolean runningWithDB, boolean writeOutInterfaces)
  {
    if (logger.isInfoEnabled()) {
      logger.info("Startup file exists and no In-memory object");
    }
    boolean dataInFileIsIncorrect = false;
    
    String tempPassword = (String)tempProps.get(NodeStartUp.InitializerParams.Params.getWAClusterPassword());
    
    String dbLoc = (String)tempProps.get(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryLocation());
    
    String dbName = (String)tempProps.get(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryDBname());
    
    String dbUname = (String)tempProps.get(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryUname());
    
    String dbPass = (String)tempProps.get(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryPass());
    
    String interfaces = (String)tempProps.get(NodeStartUp.InitializerParams.Params.getInterfaces());
    
    String lKEY = (String)tempProps.get(NodeStartUp.InitializerParams.Params.getLicenceKey());
    
    String pKEY = (String)tempProps.get(NodeStartUp.InitializerParams.Params.getProductKey());
    
    String compName = (String)tempProps.get(NodeStartUp.InitializerParams.Params.getCompanyName());
    
    Properties properties = new Properties();
    if (logger.isInfoEnabled()) {
      logger.info("Writing out new Interface");
    }
    String intfs = (String)tempProps.get(NodeStartUp.InitializerParams.Params.getInterfaces());
    if (intfs != null)
    {
      if (intfs.isEmpty())
      {
        intfs = this.bindingInterface;
        dataInFileIsIncorrect = true;
      }
      else if (!intfs.contains(this.bindingInterface))
      {
        intfs = intfs + "," + this.bindingInterface;
        dataInFileIsIncorrect = true;
      }
    }
    else
    {
      intfs = this.bindingInterface;
      dataInFileIsIncorrect = true;
    }
    if (runningWithDB)
    {
      MetaDataDBDetails dbChecking = new MetaDataDBDetails();
      boolean dbpresent;
      try
      {
        dbpresent = dbChecking.createConnection(dbLoc, dbName, dbUname, dbPass);
      }
      catch (SQLException e)
      {
        logger.error(e.getMessage(), e);
        dbpresent = false;
      }
      Map<String, String> dbDetails = null;
      if (!dbpresent) {
        try
        {
          dbDetails = dbChecking.takeDBDetails();
          dataInFileIsIncorrect = true;
        }
        catch (IOException e)
        {
          logger.error("Problem with getting DB details from command line : " + e.getMessage());
        }
      }
      dbChecking.closeConnection();
      if (logger.isInfoEnabled()) {
        logger.info("got the db details right");
      }
      if (!dbpresent)
      {
        this.initializer = new MetaInfo.Initializer();
        this.initializer.construct(cName, tempPassword, (String)dbDetails.get(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryLocation()), (String)dbDetails.get(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryDBname()), (String)dbDetails.get(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryUname()), (String)dbDetails.get(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryPass()), pKEY, compName, lKEY);
        
        properties.putAll(dbDetails);
      }
      else
      {
        this.initializer = new MetaInfo.Initializer();
        this.initializer.construct(cName, tempPassword, dbLoc, dbName, dbUname, dbPass, pKEY, compName, lKEY);
        if (dataInFileIsIncorrect)
        {
          properties.put(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryLocation(), dbLoc);
          
          properties.put(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryDBname(), dbName);
          
          properties.put(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryUname(), dbUname);
          
          properties.put(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryPass(), dbPass);
        }
      }
    }
    else
    {
      this.initializer = new MetaInfo.Initializer();
      this.initializer.construct(cName, tempPassword, dbLoc, dbName, dbUname, dbPass, pKEY, compName, lKEY);
    }
    if (dataInFileIsIncorrect)
    {
      properties.put(NodeStartUp.InitializerParams.Params.getInterfaces(), intfs);
      properties.put(NodeStartUp.InitializerParams.Params.getWAClusterName(), cName);
      properties.put(NodeStartUp.InitializerParams.Params.getWAClusterPassword(), tempPassword);
      
      properties.put(NodeStartUp.InitializerParams.Params.getLicenceKey(), lKEY);
      properties.put(NodeStartUp.InitializerParams.Params.getProductKey(), pKEY);
      properties.put(NodeStartUp.InitializerParams.Params.getCompanyName(), compName);
    }
    if (!properties.isEmpty()) {
      writePropertiesToFile(properties);
    } else if (logger.isInfoEnabled()) {
      logger.info("@startUpFileExistsAndObjectNotInMemory : No new Properties created");
    }
    startUpMap.put(cName, this.initializer);
    if (logger.isInfoEnabled()) {
      logger.info("start up map : " + startUpMap.entrySet());
    }
  }
  
  public void noStartUpFileExistsAndObjectInMemory(String cName, Map<String, String> clusterDetails, boolean runningWithDB, IMap<String, MetaInfo.Initializer> startUpMap)
  {
    if (logger.isInfoEnabled()) {
      logger.info("In-memory object available");
    }
    this.initializer = ((MetaInfo.Initializer)startUpMap.get(cName));
    String tempPassword = (String)clusterDetails.get(NodeStartUp.InitializerParams.Params.getWAClusterPassword());
    
    boolean authorized = this.initializer.authenticate(cName, tempPassword);
    while (!authorized)
    {
      logger.error("Invalid password");
      tempPassword = null;
      tempPassword = takePassword(cName);
      authorized = this.initializer.authenticate(tempPassword);
    }
    if (logger.isInfoEnabled()) {
      logger.info("Verified to join " + cName);
    }
    Properties properties = new Properties();
    properties.put(NodeStartUp.InitializerParams.Params.getWAClusterName(), cName);
    properties.put(NodeStartUp.InitializerParams.Params.getWAClusterPassword(), tempPassword);
    
    properties.put(NodeStartUp.InitializerParams.Params.getInterfaces(), this.bindingInterface);
    
    properties.put(NodeStartUp.InitializerParams.Params.getProductKey(), this.initializer.ProductKey);
    
    properties.put(NodeStartUp.InitializerParams.Params.getLicenceKey(), this.initializer.LicenseKey);
    
    properties.put(NodeStartUp.InitializerParams.Params.getCompanyName(), this.initializer.CompanyName);
    if (runningWithDB)
    {
      properties.put(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryLocation(), this.initializer.MetaDataRepositoryLocation);
      
      properties.put(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryDBname(), this.initializer.MetaDataRepositoryDBname);
      
      properties.put(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryUname(), this.initializer.MetaDataRepositoryUname);
      
      properties.put(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryPass(), this.initializer.MetaDataRepositoryPass);
    }
    writePropertiesToFile(properties);
  }
  
  public void noStartUpFileExistsAndObjectNotInMemory(Map<String, String> clusterDetails, boolean runningWithDB, IMap<String, MetaInfo.Initializer> startUpMap)
  {
    Properties properties = new Properties();
    if (logger.isInfoEnabled()) {
      logger.info("No In-memory object");
    }
    properties.putAll(clusterDetails);
    properties.put(NodeStartUp.InitializerParams.Params.getInterfaces(), this.bindingInterface);
    if (runningWithDB)
    {
      Map<String, String> dbDetails = null;
      
      MetaDataDBDetails dbChecking = new MetaDataDBDetails();
      try
      {
        dbDetails = dbChecking.takeDBDetails();
      }
      catch (IOException e)
      {
        logger.error("Problem with writing out new properties(cluster details) to disk : " + e.getMessage());
      }
      if (logger.isInfoEnabled()) {
        logger.info("got the db details right");
      }
      dbChecking.closeConnection();
      properties.putAll(dbDetails);
      
      this.initializer = new MetaInfo.Initializer();
      this.initializer.construct((String)clusterDetails.get(NodeStartUp.InitializerParams.Params.getWAClusterName()), (String)clusterDetails.get(NodeStartUp.InitializerParams.Params.getWAClusterPassword()), (String)dbDetails.get(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryLocation()), (String)dbDetails.get(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryDBname()), (String)dbDetails.get(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryUname()), (String)dbDetails.get(NodeStartUp.InitializerParams.Params.getMetaDataRepositoryPass()), (String)clusterDetails.get(NodeStartUp.InitializerParams.Params.getProductKey()), (String)clusterDetails.get(NodeStartUp.InitializerParams.Params.getCompanyName()), (String)clusterDetails.get(NodeStartUp.InitializerParams.Params.getLicenceKey()));
    }
    else
    {
      this.initializer = new MetaInfo.Initializer();
      this.initializer.construct((String)clusterDetails.get(NodeStartUp.InitializerParams.Params.getWAClusterName()), (String)clusterDetails.get(NodeStartUp.InitializerParams.Params.getWAClusterPassword()), null, null, null, null, (String)clusterDetails.get(NodeStartUp.InitializerParams.Params.getProductKey()), (String)clusterDetails.get(NodeStartUp.InitializerParams.Params.getCompanyName()), (String)clusterDetails.get(NodeStartUp.InitializerParams.Params.getLicenceKey()));
    }
    writePropertiesToFile(properties);
    startUpMap.put(clusterDetails.get(NodeStartUp.InitializerParams.Params.getWAClusterName()), this.initializer);
    this.initializer.currentData();
    if (logger.isInfoEnabled()) {
      logger.info("start up map : " + startUpMap.entrySet());
    }
  }
  
  public String takePassword(String name)
  {
    String password = null;
    int count = 0;
    boolean verified = false;
    try
    {
      String pass1 = "";
      while (pass1.equals(""))
      {
        pass1 = ConsoleReader.readPassword("Enter Cluster Password for Cluster " + name + " : ");
        if (pass1 == null)
        {
          System.err.println("No Standard Input available, hence terminating.");
          System.exit(0);
        }
      }
      password = encrypt(pass1, name);
      while (count < 3)
      {
        String pass2 = ConsoleReader.readPassword("Re-enter the Password : ");
        if (pass1.equals(pass2))
        {
          if (logger.isInfoEnabled()) {
            logger.info("Accepted");
          }
          verified = true;
          break;
        }
        System.out.println("Password did not match");
        verified = false;
        
        count++;
      }
      if (!verified)
      {
        logger.error("Cluster Password was re-entered incorrectly 3 times, please restart the system");
        System.exit(1);
      }
    }
    catch (IOException e)
    {
      logger.error(e.getLocalizedMessage());
    }
    return password;
  }
  
  public Map<String, String> takeClusterDetails()
  {
    String pass = null;
    String password = null;
    pass = System.getProperty("com.bloom.config.password");
    String clusterName = getClusterName();
    if ((pass != null) && (!pass.isEmpty()))
    {
      password = encrypt(pass, clusterName);
    }
    else
    {
      System.out.println("Required property \"Cluster Password\" is undefined");
      password = takePassword(clusterName);
    }
    Map<String, String> clusterDetails = new HashMap();
    clusterDetails.put(NodeStartUp.InitializerParams.Params.getWAClusterName(), clusterName);
    clusterDetails.put(NodeStartUp.InitializerParams.Params.getWAClusterPassword(), password);
    return clusterDetails;
  }
  
  public static String getClusterName()
  {
    String clusterName = System.getProperty("com.bloom.config.clusterName");
    if ((clusterName != null) && (!clusterName.isEmpty()))
    {
      if (logger.isInfoEnabled()) {
        logger.info("Using cluster name : " + clusterName + ", passed through system properties");
      }
    }
    else
    {
      System.out.println("Required property \"Cluster Name\" is undefined");
      try
      {
        String username = System.getProperty("user.name");
        String defaultClause = (username != null) && (!username.isEmpty()) ? "[default " + username + " (Press Enter/Return to default)]" : "";
        clusterName = ConsoleReader.readLineRespond("Enter Cluster Name " + defaultClause + " : ");
        if ((clusterName == null) || (clusterName.isEmpty())) {
          if ((username == null) || (username.isEmpty()))
          {
            System.out.println("Cluster Name was not provided, please restart the system.");
            System.exit(0);
          }
          else
          {
            clusterName = username;
          }
        }
      }
      catch (IOException e)
      {
        logger.error(e.getLocalizedMessage());
      }
    }
    return clusterName;
  }
  
  public Map<String, String> takeLicenseKeyInfo(Properties props)
    throws IOException
  {
    String clusterName = (String)props.get(NodeStartUp.InitializerParams.Params.getWAClusterName());
    if ((clusterName == null) || (clusterName.isEmpty())) {
      clusterName = getClusterName();
    }
    String companyName = System.getProperty("com.bloom.config.company_name");
    
    String licenseKey = System.getProperty("com.bloom.config.license_key");
    
    String okProductKey = System.getProperty("com.bloom.config.product_key");
    
    Path path = Paths.get(getPlatformHome(), new String[0]);
    BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class, new LinkOption[0]);
    
    FileTime creationTime = attributes.lastModifiedTime();
    
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    String date = dateFormat.format(Long.valueOf(creationTime.toMillis()));
    
    long ctime = 0L;
    try
    {
      ctime = dateFormat.parse(date).getTime();
    }
    catch (ParseException e1)
    {
      logger.error(e1.getLocalizedMessage());
    }
    if ((companyName == null) || (companyName.isEmpty())) {
      companyName = (String)props.get(NodeStartUp.InitializerParams.Params.getCompanyName());
    }
    if ((companyName == null) || (companyName.isEmpty()))
    {
      System.out.println("Required property \"Company Name\" is undefined");
      companyName = ConsoleReader.readLineRespond("Enter Company Name : ");
    }
    if ((okProductKey == null) || (okProductKey.isEmpty())) {
      okProductKey = (String)props.get(NodeStartUp.InitializerParams.Params.getProductKey());
    }
    if ((okProductKey == null) || (okProductKey.isEmpty())) {
      okProductKey = LicenseManager.get().createProductKey(companyName, clusterName, ctime);
    }
    System.out.println("Product Key " + okProductKey + " registered to " + companyName);
    
    Map<String, String> licenseKeyDetails = new HashMap();
    if ((licenseKey == null) || (licenseKey.isEmpty())) {
      licenseKey = (String)props.get(NodeStartUp.InitializerParams.Params.getLicenceKey());
    }
    if ((licenseKey == null) || (licenseKey.isEmpty()))
    {
      licenseKey = ConsoleReader.readLine("Enter License Key : (will generate trial license key if left empty). ");
      if ((licenseKey == null) || (licenseKey.isEmpty())) {
        licenseKey = LicenseManager.get().createTrialLicenseKey(clusterName, okProductKey);
      }
    }
    try
    {
      LicenseManager lm = LicenseManager.get();
      LicenseManager.testLicense(lm, "Test", companyName, okProductKey, licenseKey, null, false);
      if (!lm.allowsOption(LicenseManager.Option.CompanyLicense)) {
        throw new RuntimeException();
      }
    }
    catch (RuntimeException ignored)
    {
      try
      {
        LicenseManager.testLicense(LicenseManager.get(), "Test", clusterName, okProductKey, licenseKey, null, false);
      }
      catch (RuntimeException e)
      {
        System.out.println(e.getMessage());
        System.exit(1);
      }
    }
    licenseKeyDetails.put(NodeStartUp.InitializerParams.Params.getProductKey(), okProductKey);
    
    licenseKeyDetails.put(NodeStartUp.InitializerParams.Params.getLicenceKey(), licenseKey);
    
    licenseKeyDetails.put(NodeStartUp.InitializerParams.Params.getCompanyName(), companyName);
    
    return licenseKeyDetails;
  }
  
  public boolean isNodeStarted()
  {
    return this.nodeStarted;
  }
  
  public void setNodeStarted(boolean nodeStarted)
  {
    this.nodeStarted = nodeStarted;
  }
  
  public static class InitializerParams
  {
    public static enum Params
    {
      WAClusterName,  WAClusterPassword,  MetaDataRepositoryLocation,  MetaDataRepositoryDBname,  MetaDataRepositoryUname,  MetaDataRepositoryPass,  Interfaces,  ProductKey,  LicenceKey,  CompanyName;
      
      private Params() {}
      
      public static String getWAClusterName()
      {
        return WAClusterName.toString();
      }
      
      public static String getWAClusterPassword()
      {
        return WAClusterPassword.toString();
      }
      
      public static String getMetaDataRepositoryLocation()
      {
        return MetaDataRepositoryLocation.toString();
      }
      
      public static String getMetaDataRepositoryDBname()
      {
        return MetaDataRepositoryDBname.toString();
      }
      
      public static String getMetaDataRepositoryUname()
      {
        return MetaDataRepositoryUname.toString();
      }
      
      public static String getMetaDataRepositoryPass()
      {
        return MetaDataRepositoryPass.toString();
      }
      
      public static String getInterfaces()
      {
        return Interfaces.toString();
      }
      
      public static String getProductKey()
      {
        return ProductKey.toString();
      }
      
      public static String getLicenceKey()
      {
        return LicenceKey.toString();
      }
      
      public static String getCompanyName()
      {
        return CompanyName.toString();
      }
    }
  }
}
