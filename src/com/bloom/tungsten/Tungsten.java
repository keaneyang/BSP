package com.bloom.tungsten;

import com.bloom.cache.CacheConfiguration;
import com.bloom.cache.CacheManager;
import com.bloom.cache.CachingProvider;
import com.bloom.cache.ICache;
import com.bloom.cache.ICache.CacheInfo;
import com.bloom.classloading.WALoader;
import com.bloom.distribution.WAQueue;
import com.bloom.distribution.WAQueue.Listener;
import com.bloom.event.QueryResultEvent;
import com.bloom.exception.FatalException;
import com.bloom.exception.SecurityException;
import com.bloom.exception.Warning;
import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.metaRepository.MDClientOps;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.metaRepository.HazelcastSingleton.BloomClusterMemberType;
import com.bloom.proc.events.ShowStreamEvent;
import com.bloom.runtime.ConsoleReader;
import com.bloom.runtime.Context;
import com.bloom.runtime.DistributedExecutionManager;
import com.bloom.runtime.NodeStartUp;
import com.bloom.runtime.PartitionManager;
import com.bloom.runtime.ShowStreamManager;
import com.bloom.runtime.Version;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.compiler.Compiler.ExecutionCallback;
import com.bloom.runtime.compiler.stmts.ConnectStmt;
import com.bloom.runtime.compiler.stmts.CreatePropertySetStmt;
import com.bloom.runtime.compiler.stmts.CreatePropertyVariableStmt;
import com.bloom.runtime.compiler.stmts.CreateUserStmt;
import com.bloom.runtime.compiler.stmts.DumpStmt;
import com.bloom.runtime.compiler.stmts.LoadFileStmt;
import com.bloom.runtime.compiler.stmts.Stmt;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.components.Stream;
import com.bloom.runtime.containers.TaskEvent;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfoStatus;
import com.bloom.runtime.meta.MetaInfo.Cache;
import com.bloom.runtime.meta.MetaInfo.DeploymentGroup;
import com.bloom.runtime.meta.MetaInfo.Initializer;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.Namespace;
import com.bloom.runtime.meta.MetaInfo.Query;
import com.bloom.runtime.meta.MetaInfo.Role;
import com.bloom.runtime.meta.MetaInfo.Server;
import com.bloom.runtime.meta.MetaInfo.Target;
import com.bloom.runtime.meta.MetaInfo.Type;
import com.bloom.runtime.meta.MetaInfo.User;
import com.bloom.security.SessionInfo;
import com.bloom.security.WASecurityManager;
import com.bloom.usagemetrics.tungsten.Usage;
import com.bloom.utility.Utility;
import com.bloom.uuid.AuthToken;
import com.bloom.uuid.UUID;
import com.fasterxml.jackson.databind.JsonNode;
import com.hazelcast.core.Endpoint;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.core.Member;
import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.WAEvent;

import java.io.BufferedReader;
import java.io.Console;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.InetSocketAddress;
import java.nio.MappedByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Deque;
import java.util.EnumMap;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class Tungsten
{
  private static Logger logger = Logger.getLogger(Tungsten.class);
  private static String WA_LOG = "WA.LOG";
  public String commandLineFormat = "W (%A) > ";
  public static AtomicBoolean isAdhocRunning = new AtomicBoolean(false);
  public static AtomicBoolean isQueryDumpOn = new AtomicBoolean(false);
  static Map<Keywords, String> enumMap = new EnumMap(Keywords.class);
  boolean isBatch = false;
  private static String SPOOL_ERROR_MSG = "Error on spool command.\n";
  static File file;
  private static boolean insinglequote;
  private static boolean indoublequote;
  static MetaInfo.Query query = null;
  public static IMap<UUID, UUID> nodeIDToAuthToken;
  static Context ctx = null;
  static boolean isCreated = true;
  public static AuthToken session_id = null;
  public static MetaInfo.User currUserMetaInfo = null;
  public static DateTimeZone userTimeZone = null;
  static String comments = "Tungsten Property File";
  public static boolean quit = false;
  
  public static enum PrintFormat
  {
    JSON,  ROW_FORMAT;
    
    private PrintFormat() {}
  }
  
  public static PrintFormat currentFormat = PrintFormat.JSON;
  private static boolean isShuttingDown = false;
  static MDClientOps clientOps;
  static AtomicInteger NUM_OF_ERRORS = new AtomicInteger(0);
  private static WAQueue sessionQueue;
  private static WAQueue.Listener queuelistener;
  private static SimpleWebSocketClient tungsteClient = null;
  private static String serverSocketUrl = "ws://localhost:9080/rmiws/";
  
  public static void setSessionQueue(WAQueue sessionQueue)
  {
    sessionQueue = sessionQueue;
  }
  
  public static void setQueuelistener(WAQueue.Listener queuelistener)
  {
    queuelistener = queuelistener;
  }
  
  public static enum Keywords
  {
    HELP,  PREV,  HISTORY,  GO,  CONNECT,  SET,  SHOW,  QUIT,  EXIT,  LIST,  DESCRIBE,  MONITOR;
    
    private Keywords() {}
  }
  
  public Tungsten()
  {
    enumMap.put(Keywords.HELP, "[Lists this help menu]\n            'help <objectType>' - Gives example of object type e.g 'help type'");
    enumMap.put(Keywords.PREV, "[Prints and repeats the previous command]");
    enumMap.put(Keywords.HISTORY, "[Prints all commands in this session]\n            'history clear' - Deletes the history\n            'history x' - Executes command at number x");
    enumMap.put(Keywords.GO, "[End batch scripts after batchmode]");
    enumMap.put(Keywords.CONNECT, "usage: CONNECT username password [TO CLUSTER clustername]\n           connects as the user to the current cluster [or the specified cluster]");
    enumMap.put(Keywords.SET, "[Set options such as: batchmode...etc]");
    enumMap.put(Keywords.SHOW, "[Show stream e.g. \"show s1\"]");
    enumMap.put(Keywords.QUIT, "[Quit]");
    enumMap.put(Keywords.EXIT, "[Quit]");
    enumMap.put(Keywords.LIST, "[List metadata objects by type e.g. \"list types\"]");
    enumMap.put(Keywords.DESCRIBE, "[Describe metadata objects by given name e.g. \"describe typename\"]");
    enumMap.put(Keywords.MONITOR, "usage: monitor command [id] [-n info-names] [-v]\n         command: cluster, entity, follow, count, clear\n              id: a three-part string in the format APPNAME.ENTITY-TYPE.ENTITY-NAME\n   -n info-names: a list of names separated by commas (but not spaces!)\n                  ex: -n input-total,output-total,processed\n              -v: verbose, prints extra details");
  }
  
  static MappedByteBuffer out = null;
  static int SIZE = 11024;
  public static Stream showStream;
  public static final String CLASS = "class";
  public static final String METHOD = "method";
  public static final String PARAMS = "params";
  public static final String CALLBACK = "callback";
  public static final String TYPE = "Tungsten";
  
  public static void argsController(String[] args)
  {
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-batchfile"))
      {
        if (args.length == i + 1)
        {
          printf("\n No Filename given!");
          return;
        }
        String scriptFilename = args[(i + 1)];
        File fname = new File(scriptFilename);
        if (!fname.exists())
        {
          printf("\nFile " + scriptFilename + " does not exist!\n");
          return;
        }
        FileReader in = null;
        try
        {
          in = new FileReader(scriptFilename);
        }
        catch (FileNotFoundException e)
        {
          e.printStackTrace();
        }
        StringBuilder contents = new StringBuilder();
        char[] buffer = new char[4096];
        int read = 0;
        do
        {
          contents.append(buffer, 0, read);
          try
          {
            read = in.read(buffer);
          }
          catch (IOException e)
          {
            e.printStackTrace();
          }
        } while (read >= 0);
        try
        {
          in.close();
        }
        catch (IOException e) {}
        String stripped = contents.toString().replaceAll("(?m)^((?:(?!--|').|'(?:''|[^'])*')*)--.*$", "$1");
        process(stripped.toString());
      }
    }
  }
  
  static UUID userAuth()
    throws IOException
  {
    String uname = System.getProperty("com.bloom.config.username");
    String password = System.getProperty("com.bloom.config.password");
    for (int i = 0; i < 3; i++)
    {
      while ((uname == null) || (uname.isEmpty())) {
        uname = ConsoleReader.readLine("Username : ");
      }
      while ((password == null) || (password.isEmpty())) {
        password = ConsoleReader.readPassword("Password (for " + uname + "): ");
      }
      try
      {
        String clientId = HazelcastSingleton.get().getLocalEndpoint().getUuid();
        session_id = clientOps.authenticate(uname, password, clientId, "Tungsten");
        if (session_id != null)
        {
          currUserMetaInfo = (MetaInfo.User)clientOps.getMetaObjectByName(EntityType.USER, "Global", uname, null, session_id);
          try
          {
            TimeZone jtz = TimeZone.getTimeZone(currUserMetaInfo.getUserTimeZone());
            userTimeZone = currUserMetaInfo.getUserTimeZone().equals("") ? null : DateTimeZone.forTimeZone(jtz);
          }
          catch (Exception e)
          {
            printf("Unable to use user's timezone - " + currUserMetaInfo.getUserTimeZone() + ", defaulting to local timezone");
          }
          return session_id;
        }
      }
      catch (Exception e)
      {
        logger.error(e);
        printf("Invalid username or password.\n");
        uname = password = null;
      }
    }
    System.exit(NUM_OF_ERRORS.get());
    return null;
  }
  
  public static void printf(String toPrint)
  {
    synchronized (System.out)
    {
      if (System.console() != null)
      {
        String escapedToPrint = toPrint.replace("%", "%%");
        System.console().printf(escapedToPrint, new Object[0]);
      }
      else
      {
        System.out.print(toPrint);
      }
      System.out.flush();
    }
  }
  
  private static void shutDown()
  {
    if (ctx != null)
    {
      isShuttingDown = true;
      Context.shutdown("Tungsten ...");
    }
  }
  
  private static void initialize()
    throws Exception
  {
    String cName = System.getProperty("com.bloom.config.clusterName");
    if ((cName == null) || (cName.isEmpty()))
    {
      cName = NodeStartUp.getClusterName();
      System.setProperty("com.bloom.config.clusterName", cName);
    }
    try
    {
      HazelcastInstance HZ = HazelcastSingleton.get(cName, HazelcastSingleton.BloomClusterMemberType.CONSOLENODE);
      
      clientOps = MDClientOps.getINSTANCE();
      
      HZ.getLifecycleService().addLifecycleListener(new LifecycleListener()
      {
        public void stateChanged(LifecycleEvent event)
        {
          if (event.getState().equals(LifecycleEvent.LifecycleState.SHUTTING_DOWN))
          {
            Tungsten.ctx = null;
            if (!Tungsten.isShuttingDown)
            {
              System.out.println("\nNo available servers in cluster, shutting down.");
              System.exit(Tungsten.NUM_OF_ERRORS.get());
            }
          }
        }
      });
      WALoader.get();
      printf(".");
      printf(".");
      
      printf(".");
      try
      {
        WASecurityManager.setFirstTime(false);
        printf(".");
      }
      catch (SecurityException e)
      {
        if (logger.isDebugEnabled()) {
          logger.debug(e.getMessage());
        }
      }
      printf("connected.\n");
      
      userAuth();
      
      HazelcastSingleton.activeClusterMembers.put(HazelcastSingleton.getNodeId(), HazelcastSingleton.get().getLocalEndpoint());
      initShowStream();
      setSessionQueue(WAQueue.getQueue("consoleQueue" + session_id));
      setQueuelistener(new WAQueue.Listener()
      {
        public void onItem(Object item)
        {
          if (Tungsten.isAdhocRunning.get() == true) {
            Tungsten.prettyPrintEvent(item);
          } else if ((item instanceof JsonNode)) {
            if (Tungsten.isQueryDumpOn.get() == true)
            {
              JsonNode jNode = (JsonNode)item;
              System.out.print("Source:" + jNode.get("datasource"));
              System.out.print(",IO:" + jNode.get("IO"));
              System.out.print(",Action:" + jNode.get("type"));
              System.out.print(",Server:" + jNode.get("serverid"));
              System.out.println(",Data:" + jNode.get("data"));
            }
          }
        }
      });
      getSessionQueue().subscribeForTungsten(getQueuelistener());
    }
    catch (Exception e)
    {
      logger.warn(e.getMessage());
      printf("No Server found in the cluster : " + cName + ". \n");
      System.exit(-1);
    }
  }
  
  public static void prettyPrintEvent(Object e)
  {
    if ((((TaskEvent)e).getQueryID() != null) && (query.getUuid() != null) && (!((TaskEvent)e).getQueryID().equals(query.getUuid())))
    {
      if (logger.isInfoEnabled()) {
        logger.info("Old query result ignored.");
      }
      return;
    }
    if ((e instanceof TaskEvent))
    {
      IBatch<WAEvent> waEventIBatch = ((TaskEvent)e).batch();
      if (currentFormat == PrintFormat.ROW_FORMAT)
      {
        CluiMonitorView.printTableTitle("Query Results");
        if ((((TaskEvent)e).batch() != null) && (!((TaskEvent)e).batch().isEmpty()) && ((QueryResultEvent)((WAEvent)waEventIBatch.first()).data != null))
        {
          printTableRow(((QueryResultEvent)((WAEvent)waEventIBatch.first()).data).fieldsInfo);
          CluiMonitorView.printTableDivider();
        }
      }
      for (WAEvent b : waEventIBatch) {
        if (currentFormat == PrintFormat.ROW_FORMAT)
        {
          rowPrintData((QueryResultEvent)b.data);
        }
        else if (currentFormat == PrintFormat.JSON)
        {
          multiPrint("[\n");
          printQueryEvent(b.data);
          multiPrint("]\n");
        }
      }
    }
    else if ((e instanceof QueryResultEvent))
    {
      multiPrint("[\n");
      printQueryEvent(e);
      multiPrint("]\n");
    }
    else if ((e instanceof ShowStreamEvent))
    {
      multiPrint("[\n");
      multiPrint(((ShowStreamEvent)e).event.toString());
      multiPrint("]\n");
    }
  }
  
  private static void rowPrintData(QueryResultEvent qre)
  {
    printTableRow(qre.getDataPoints());
    CluiMonitorView.printTableDivider();
  }
  
  private static void printTableRow(Object[] columnNames)
  {
    StringBuilder sb = new StringBuilder();
    int TMP_WIDTH = 132 / columnNames.length;
    int MIN_COL = TMP_WIDTH;
    int sum = 0;
    for (int i = 0; i < columnNames.length; i++)
    {
      if (i + 1 == columnNames.length)
      {
        int delta = 132 - sum;
        TMP_WIDTH = delta;
      }
      sum += TMP_WIDTH;
      sb.append("%-" + TMP_WIDTH + "s");
    }
    String formatter = "= " + sb.toString() + "=%n";
    for (int i = 0; i < columnNames.length; i++) {
      if ((columnNames[i] != null) && (columnNames[i].toString().length() > MIN_COL))
      {
        String columnString = columnNames[i].toString();
        columnNames[i] = (columnNames[i].toString().substring(0, MIN_COL / 2 - 2) + "~" + columnNames[i].toString().substring(columnString.length() - MIN_COL / 2 + 2, columnString.length()).trim());
      }
    }
    System.out.format(formatter, columnNames);
  }
  
  private static void printQueryEvent(Object e)
  {
    if ((e instanceof QueryResultEvent))
    {
      QueryResultEvent qre = (QueryResultEvent)e;
      int i = 0;
      Object[] payload = qre.getPayload();
      String[] fieldNames = qre.getFieldsInfo();
      for (String fieldName : fieldNames)
      {
        if ((payload[i] == null) || (payload[i].getClass() == null)) {
          multiPrint("   " + fieldName + " = null \n");
        } else if (payload[i].getClass().equals(byte[].class)) {
          multiPrint("   " + fieldName + " = " + Arrays.toString((byte[])payload[i]) + "\n");
        } else if (payload[i].getClass().isArray()) {
          multiPrint("   " + fieldName + " = " + Arrays.toString((Object[])payload[i]) + "\n");
        } else if ((payload[i].getClass().equals(DateTime.class)) && (userTimeZone != null)) {
          multiPrint("   " + fieldName + " = " + new DateTime(payload[i], userTimeZone) + "\n");
        } else {
          multiPrint("   " + fieldName + " = " + payload[i] + "\n");
        }
        i++;
      }
    }
  }
  
  protected static MetaInfo.Type getTypeForCLass(Class<?> clazz)
    throws MetaDataRepositoryException
  {
    MetaInfo.Type type = null;
    String typeName = "Global." + clazz.getSimpleName();
    try
    {
      type = (MetaInfo.Type)clientOps.getMetaObjectByName(EntityType.TYPE, "Global", clazz.getSimpleName(), null, WASecurityManager.TOKEN);
    }
    catch (Exception e)
    {
      if (logger.isInfoEnabled()) {
        logger.info(e.getLocalizedMessage());
      }
      return null;
    }
    if (type == null)
    {
      Map<String, String> fields = new LinkedHashMap();
      Field[] cFields = clazz.getDeclaredFields();
      for (Field f : cFields) {
        if (Modifier.isPublic(f.getModifiers())) {
          fields.put(f.getName(), f.getType().getCanonicalName());
        }
      }
      type = new MetaInfo.Type();
      type.construct(typeName, MetaInfo.GlobalNamespace, clazz.getName(), fields, null, false);
    }
    return type;
  }
  
  public static void initShowStream()
  {
    try
    {
      if (logger.isInfoEnabled()) {
        logger.info("Creating SHOW Stream");
      }
      MetaInfo.Type dataType = getTypeForCLass(ShowStreamEvent.class);
      MetaInfo.Stream streamMetaObj = new MetaInfo.Stream();
      streamMetaObj.construct("showStream", ShowStreamManager.ShowStreamUUID, MetaInfo.GlobalNamespace, dataType.uuid, null, null, null);
      
      showStream = new Stream(streamMetaObj, null, null);
      showStream.start();
      if (logger.isDebugEnabled()) {
        logger.debug("exception stream, control stream are created");
      }
    }
    catch (Exception se)
    {
      logger.error("error" + se);
      se.printStackTrace();
    }
  }
  
  private static void multiPrint(String msg)
  {
    synchronized (System.out)
    {
      if (System.console() != null)
      {
        String escapedToPrint = msg.replace("%", "%%");
        System.console().printf(escapedToPrint, new Object[0]);
      }
      else
      {
        System.out.print(msg);
      }
      System.out.flush();
    }
    if ((SpoolFile.fileName != null) && (SpoolFile.spoolMode.get() == true))
    {
      if (SpoolFile.writer == null) {
        try
        {
          SpoolFile.writer = new PrintWriter(SpoolFile.fileName, "UTF-8");
        }
        catch (FileNotFoundException|UnsupportedEncodingException e)
        {
          logger.error(e.getMessage());
        }
      }
      if (SpoolFile.writer != null) {
        SpoolFile.writer.print(msg);
      }
    }
  }
  
  public static void checkAndCleanupAdhoc(Boolean stopShowStream)
  {
    try
    {
      if (stopShowStream.booleanValue()) {
        showStream.stop();
      }
      getSessionQueue().unsubscribe(getQueuelistener());
    }
    catch (Exception e)
    {
      if (logger.isInfoEnabled()) {
        logger.info(e.getMessage());
      }
    }
  }
  
  public static void main(String[] args)
    throws Exception
  {
    printf("Welcome to the Bloom Tungsten Command Line - " + Version.getVersionString() + "\n");
    
    initialize();
    nodeIDToAuthToken = HazelcastSingleton.get().getMap("#nodeIDToAuthToken");
    nodeIDToAuthToken.put(HazelcastSingleton.getNodeId(), session_id);
    
    ctx = Context.createContext(session_id);
    
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable()
    {
      public void run() {}
    }));
    String isShutDown = System.getProperty("com.bloom.config.shutdown");
    if ((isShutDown != null) && (isShutDown.equals("true")))
    {
      if (currUserMetaInfo.getName().equalsIgnoreCase("admin"))
      {
        String actionstr = null;
        String allcluster = System.getProperty("com.bloom.config.shutallcluster");
        if ((allcluster != null) && (allcluster.equalsIgnoreCase("true")))
        {
          actionstr = "ALL";
        }
        else
        {
          String ipAddress = System.getProperty("com.bloom.config.interface");
          String port = System.getProperty("com.bloom.config.port");
          if ((ipAddress == null) || (port == null))
          {
            printf("IP Address and Port must be specified for server shutdown\n");
            System.exit(NUM_OF_ERRORS.get());
          }
          actionstr = "/" + ipAddress + ":" + port;
        }
        printf("Initiated shutdown\n");
        try
        {
          ctx.executeShutdown(actionstr);
        }
        catch (Throwable e)
        {
          printf("ERROR:" + e.getLocalizedMessage() + "\n");
        }
      }
      else
      {
        printf("Only Admin user can initiate shutdown\n");
        System.exit(NUM_OF_ERRORS.get());
      }
      System.exit(NUM_OF_ERRORS.get());
      return;
    }
    process("use " + currUserMetaInfo.getDefaultNamespace() + ";");
    
    argsController(args);
    
    Tungsten console = new Tungsten();
    
    String clFormat = System.getProperty("com.bloom.config.prompt-format");
    if ((clFormat != null) && (!clFormat.isEmpty())) {
      console.commandLineFormat = clFormat;
    }
    console.offerHelp();
    
    Deque<String> history = new ArrayDeque();
    ConsoleReader.enableHistory();
    while (!quit) {
      try
      {
        String cmd = ConsoleReader.readLine(console.getPrompt());
        if (cmd == null) {
          break;
        }
        if (!cmd.isEmpty())
        {
          if (cmd.toUpperCase().startsWith("PREV")) {
            cmd = "HISTORY 0";
          }
          if (cmd.toUpperCase().startsWith("HISTORY"))
          {
            String result = handleHistory(cmd, history);
            if ((result == null) || (!result.isEmpty()))
            {
              cmd = result;
              System.out.println(cmd);
            }
          }
          else
          {
            history.addLast(cmd);
            
            String firstWord = cmd.trim().toUpperCase().split(" ")[0];
            if (firstWord.endsWith(";")) {
              firstWord = firstWord.substring(0, firstWord.length() - 1);
            }
            Map map;
            switch (firstWord)
            {
            case "BATCHMODE": 
              console.setBatchmode(cmd);
              break;
            case "HELP": 
              console.help(cmd);
              break;
            case "GO": 
              console.executeBatch();
              break;
            case "SPOOL": 
              spool(cmd);
              break;
            case "BACKUP": 
              backup(cmd);
              break;
            case "MDUMP": 
              map = clientOps.dumpMaps();
              for (Object o : map.keySet())
              {
                Object val = map.get(o);
                String strVal = null;
                if ((val instanceof MetaInfo.MetaObject)) {
                  strVal = ((MetaInfo.MetaObject)val).JSONifyString();
                } else {
                  strVal = val.toString();
                }
                multiPrint(o + ": " + strVal + "\n");
              }
              break;
            case "WACACHE": 
              dumpCaches(cmd);
              break;
            case "SLEEP": 
              String[] s = cmd.split("\\s+");
              int t = Integer.parseInt(s[1]);
              Thread.sleep(t);
              
              break;
            case "CDUMP": 
              WALoader.get().listAllBundles();
              break;
            case "CID": 
              WALoader.get().listClassAndId();
              break;
            case "MGET": 
              String[] ar = cmd.trim().split(" ");
              if (ar.length > 1)
              {
                assert (ar.length == 4);
                printf("MetaObject " + ar[1] + " = " + clientOps.getMetaObjectByName(EntityType.valueOf(ar[0]), ar[1], ar[2], Integer.getInteger(ar[3]), session_id));
              }
              break;
            case "TEST": 
              testCmd(cmd);
              break;
            case "USAGE": 
              Usage.execute(ctx, cmd);
              break;
            default: 
              isCreated = false;
              console.executeCommand(cmd);
            }
          }
        }
      }
      catch (Exception e)
      {
        logger.error("Problem executing statement", e);
      }
    }
    printf("\nQuit.");
    closeWebSocket();
    System.exit(NUM_OF_ERRORS.get());
  }
  
  static class SpoolFile
  {
    static AtomicBoolean spoolMode = new AtomicBoolean(false);
    static String fileName = null;
    static PrintWriter writer = null;
  }
  
  private static void spool(String cmd)
  {
    cmd = cmd.trim();
    if (cmd.endsWith(";")) {
      cmd = cmd.substring(0, cmd.length() - 1);
    }
    String[] parts = cmd.trim().split(" ");
    if ((parts.length > 4) || (parts.length < 2))
    {
      printf(SPOOL_ERROR_MSG);
      return;
    }
    if (parts[1].equalsIgnoreCase("ON"))
    {
      String fileName = WA_LOG;
      boolean spoolConditionsMet = false;
      if (parts.length == 2) {
        spoolConditionsMet = true;
      }
      if ((parts.length == 4) && (parts[2].equalsIgnoreCase("TO")))
      {
        fileName = parts[3];
        spoolConditionsMet = true;
      }
      if (spoolConditionsMet)
      {
        SpoolFile.spoolMode.set(true);
        SpoolFile.fileName = fileName;
        try
        {
          SpoolFile.writer = new PrintWriter(SpoolFile.fileName, "UTF-8");
        }
        catch (FileNotFoundException|UnsupportedEncodingException e)
        {
          logger.error(e.getMessage());
        }
      }
      else
      {
        printf(SPOOL_ERROR_MSG);
        return;
      }
    }
    else if (parts[1].equalsIgnoreCase("OFF"))
    {
      SpoolFile.spoolMode.set(false);
      if (SpoolFile.writer != null)
      {
        SpoolFile.writer.close();
        SpoolFile.writer = null;
      }
      SpoolFile.fileName = null;
    }
    else
    {
      printf(SPOOL_ERROR_MSG);
      return;
    }
  }
  
  private static String handleHistory(String cmd, Deque<String> history)
  {
    cmd = cmd.trim();
    if (cmd.endsWith(";")) {
      cmd = cmd.substring(0, cmd.length() - 1);
    }
    String[] parts = cmd.trim().toUpperCase().split(" ");
    if ((parts.length >= 2) && (parts[1].equalsIgnoreCase("clear")))
    {
      history.clear();
      printf("Cleared history\n");
      return null;
    }
    if ((parts.length >= 2) && (NumberUtils.isNumber(parts[1])) && (!history.isEmpty()))
    {
    	Integer ii = null;
      try
      {
        ii = Integer.valueOf(Integer.parseInt(parts[1]));
      }
      catch (NumberFormatException e)
      {
        logger.error("Bad number format, must be integer: " + parts[1]);
        return null;
      }
       ii = Integer.valueOf(ii.intValue() % history.size());
      if (ii.intValue() <= 0) {
        ii = Integer.valueOf(ii.intValue() + history.size());
      }
      Iterator<String> it = history.iterator();
      String hist_cmd = "";
      int jj = 1;
      while (it.hasNext())
      {
        if (jj == ii.intValue())
        {
          hist_cmd = (String)it.next();
          break;
        }
        it.next();
        
        jj++;
      }
      return hist_cmd;
    }
    Iterator<String> it = history.iterator();
    int num = 1;
    while (it.hasNext())
    {
      System.out.printf("%5d   %s\n", new Object[] { Integer.valueOf(num++), it.next() });
      System.out.flush();
    }
    return null;
  }
  
  private void offerHelp()
  {
    printf("Type 'HELP' to get some assistance, or enter commands below\n");
  }
  
  private void executeCommand(String cmd)
  {
    cmd = cmd + "\n";
    if ((cmd.equals(";")) || (cmd.isEmpty())) {
      return;
    }
    StringBuilder strAcc = new StringBuilder();
    
    strAcc.append(cmd + " ");
    boolean isbalanced = isBalanced(strAcc.toString());
    if ((cmd.trim().endsWith(";")) && (!this.isBatch)) {
      if (isbalanced == true)
      {
        process(strAcc.toString());
        
        return;
      }
    }
    try
    {
      String line;
      while ((line = ConsoleReader.readLine("... ")) != null)
      {
        if ((line.equals("GO")) && (this.isBatch == true))
        {
          process(strAcc.toString());
          executeBatch();
          
          break;
        }
        if ((line.trim().endsWith(";")) && (!this.isBatch))
        {
          line = line + "\n";
          strAcc.append(line);
          isbalanced = isBalanced(strAcc.toString());
          if ((!this.isBatch) && (isbalanced == true))
          {
            process(strAcc.toString());
            break;
          }
        }
        else if ((line.trim().endsWith(";")) && (this.isBatch == true))
        {
          strAcc.append(line + " ");
        }
        else
        {
          line = line + "\n";
          strAcc.append(line + " ");
        }
      }
    }
    catch (Exception e)
    {
      logger.error(e);
    }
  }
  
  public static void process(String text)
  {
    processWithContext(text, ctx);
  }
  
  public static void processWithContext(final String text, final Context ctx)
  {
    final AtomicBoolean isItLoadStatement = new AtomicBoolean(false);
    long beg = System.currentTimeMillis();
    try
    {
      Compiler.compile(text, ctx, new Compiler.ExecutionCallback()
      {
        public void execute(Stmt stmt, Compiler compiler)
          throws MetaDataRepositoryException
        {
          if ((stmt instanceof LoadFileStmt)) {
            isItLoadStatement.set(true);
          }
          if (!isItLoadStatement.get()) {
            Tungsten.printf("Processing - " + stmt.sourceText.replaceAll("%", "%%") + "\n");
          }
          try
          {
            Object result = compiler.compileStmt(stmt);
            if ((result instanceof MetaInfo.Query))
            {
              Tungsten.query = (MetaInfo.Query)result;
              if (Tungsten.query.isAdhocQuery()) {
                try
                {
                  Tungsten.isAdhocRunning.set(true);
                  ctx.startAdHocQuery(Tungsten.query);
                  ConsoleReader.readLine();
                }
                catch (Exception e)
                {
                  throw new RuntimeException(e);
                }
                finally
                {
                  ctx.stopAndDropAdHocQuery(Tungsten.query);
                  Tungsten.isAdhocRunning.set(false);
                }
              }
            }
            else if ((result instanceof DumpStmt))
            {
              DumpStmt dmpstmt = (DumpStmt)result;
              Tungsten.isQueryDumpOn.set(true);
              try
              {
                ConsoleReader.readLine();
              }
              catch (IOException e)
              {
                throw new Warning(e);
              }
              finally
              {
                Tungsten.isQueryDumpOn.set(false);
                ctx.changeCQInputOutput(dmpstmt.getCqname(), dmpstmt.getDumpmode(), 2, Tungsten.session_id);
              }
            }
          }
          catch (Warning e)
          {
            Tungsten.printf("-> FAILURE \n");
            Tungsten.NUM_OF_ERRORS.incrementAndGet();
            String msg = e.getLocalizedMessage();
            Tungsten.printf(msg + "\n");
            if (Tungsten.logger.isInfoEnabled()) {
              Tungsten.logger.info(e, e);
            }
            return;
          }
          catch (FatalException e)
          {
            throw e;
          }
          if ((!(stmt instanceof CreatePropertySetStmt)) && (!(stmt instanceof CreateUserStmt)) && (!(stmt instanceof ConnectStmt)) && (!(stmt instanceof CreatePropertyVariableStmt))) {
            if ((stmt.sourceText == null) || (stmt.sourceText.isEmpty())) {
              Tungsten.storeCommand(text, null);
            } else {
              Tungsten.storeCommand(stmt.getRedactedSourceText(), null);
            }
          }
          if (!isItLoadStatement.get()) {
            Tungsten.printf("-> SUCCESS \n");
          }
          if ((stmt.returnText != null) && (!stmt.returnText.isEmpty())) {
            for (Object obj : stmt.returnText) {
              if (((obj instanceof String)) && 
                (!((String)obj).isEmpty())) {
                Tungsten.printf((String)obj);
              }
            }
          }
        }
      });
    }
    catch (Throwable e)
    {
      printf("-> FAILURE \n");
      NUM_OF_ERRORS.incrementAndGet();
      if ((e instanceof NullPointerException))
      {
        printf("Internal error:" + e + "\n");
        for (StackTraceElement s : e.getStackTrace()) {
          printf(s + "\n");
        }
      }
      else if ((e.getCause() instanceof ClassNotFoundException))
      {
        String msg = "Class not found in the lib path : " + e.getLocalizedMessage();
        printf(msg + "\n");
      }
      else
      {
        String msg = e.getLocalizedMessage();
        printf(msg + "\n");
      }
      if (logger.isInfoEnabled()) {
        logger.info(e, e);
      }
    }
    long end = System.currentTimeMillis();
    if (!isItLoadStatement.get()) {
      printf("Elapsed time: " + (end - beg) + " ms\n");
    }
  }
  
  private static void storeCommand(String text, String methodName)
  {
    try
    {
      String json = "{\"class\":\"com.bloom.runtime.QueryValidator\", \"method\":\"storeCommand\",\"params\":[\"#sessionid#\", \"#userid#\", \"#command#\"],\"callbackIndex\":2}";
      if ((tungsteClient == null) || (!tungsteClient.isConnected()))
      {
        serverSocketUrl = "ws://" + getServerIp() + ":9080/rmiws/";
        if (logger.isInfoEnabled()) {
          logger.info("serverSocketUrl: " + serverSocketUrl);
        }
        tungsteClient = new SimpleWebSocketClient(serverSocketUrl);
        tungsteClient.start();
      }
      json = json.replace("#sessionid#", session_id.getUUIDString());
      text = text.replaceAll("(\\r|\\n)", "");
      text = StringEscapeUtils.escapeJava(text);
      json = json.replace("#command#", text);
      json = json.replace("#userid#", currUserMetaInfo.getName());
      if (methodName != null) {
        json = json.replace("storeCommand", methodName);
      }
      if (logger.isDebugEnabled()) {
        logger.debug("sending text :" + json);
      }
      tungsteClient.sendMessages(json);
    }
    catch (Exception ex)
    {
      if (logger.isDebugEnabled()) {
        logger.debug("error logging command:", ex);
      }
    }
  }
  
  private static void closeWebSocket()
  {
    tungsteClient.close();
  }
  
  private static String getServerIp()
  {
    Set<Member> srvs = DistributedExecutionManager.getFirstServer(HazelcastSingleton.get());
    if (srvs.isEmpty()) {
      throw new RuntimeException("No available Node to get Monitor data");
    }
    Iterator i$ = srvs.iterator();
    if (i$.hasNext())
    {
      Member member = (Member)i$.next();
      return member.getSocketAddress().getHostName();
    }
    return null;
  }
  
  public static void process(String text, final Context mockContext)
  {
    long beg = System.currentTimeMillis();
    try
    {
      Compiler.compile(text, mockContext, new Compiler.ExecutionCallback()
      {
        public void execute(Stmt stmt, Compiler compiler)
          throws MetaDataRepositoryException
        {
          Tungsten.printf("Processing - " + stmt.sourceText.replaceAll("%", "%%") + "\n");
          Object result = compiler.compileStmt(stmt);
          if ((result instanceof MetaInfo.Query))
          {
            MetaInfo.Query query = (MetaInfo.Query)result;
            if (query.isAdhocQuery()) {
              try
              {
                Tungsten.isAdhocRunning.set(true);
                mockContext.startAdHocQuery(query);
                ConsoleReader.readLine();
              }
              catch (Exception e)
              {
                throw new RuntimeException(e);
              }
              finally
              {
                mockContext.stopAndDropAdHocQuery(query);
                Tungsten.isAdhocRunning.set(false);
              }
            }
          }
          Tungsten.printf("-> SUCCESS \n");
        }
      });
    }
    catch (Throwable e)
    {
      printf("-> FAILURE \n");
      if ((e instanceof NullPointerException))
      {
        printf("Internal error:" + e + "\n");
        for (StackTraceElement s : e.getStackTrace()) {
          printf(s + "\n");
        }
      }
      else if ((e.getCause() instanceof ClassNotFoundException))
      {
        String msg = "Class not found in the lib path: " + e.getLocalizedMessage();
        printf(msg + "\n");
      }
      else
      {
        String msg = e.getLocalizedMessage();
        printf(msg + "\n");
      }
      if (logger.isInfoEnabled()) {
        logger.info(e, e);
      }
    }
    long end = System.currentTimeMillis();
    printf("Elapsed time: " + (end - beg) + " ms\n");
  }
  
  private void executeBatch()
  {
    this.isBatch = false;
  }
  
  private void help(String cmd)
  {
    String[] ar = cmd.split(" ");
    if (ar.length == 1)
    {
      printf("Usage: \n");
      Keywords[] menuOps = Keywords.values();
      for (int i = 0; i < menuOps.length; i++) {
        System.out.printf("%9s  %s\n", new Object[] { menuOps[i].toString(), enumMap.get(menuOps[i]) });
      }
      System.out.printf("%9s  %s\n", new Object[] { "USAGE", "[Display usage information]\n           'USAGE' displays usage information for the entire installation\n           'USAGE <application>' displays usage information for a single application" });
      System.out.flush();
      printf("        @  [Run BatchScript e.g. \"@{path}{file}\" ]\n");
      
      return;
    }
    if (ar.length == 2)
    {
      String noSemicolon = ar[1].toUpperCase();
      if (noSemicolon.endsWith(";")) {
        noSemicolon = noSemicolon.substring(0, noSemicolon.length() - 1).toUpperCase();
      }
      switch (noSemicolon)
      {
      case "TYPE": 
        printf("Example :\n");
        printf("CREATE TYPE PosData(  \n merchantId String, \n dateTime java.util.Date, \n hourValue int,  \n amount double,  \n zip String );\n");
        
        break;
      case "SOURCE": 
        printf("Example :\n");
        printf("CREATE source CsvDataSource USING CSVReader (\n  directory:'../BloomTests/data',\n  header:Yes,\n  wildcard:'posdata.csv',\n  coldelimiter:','\n) OUTPUT TO CsvStream;\n");
        
        break;
      case "STREAM": 
        printf("Example :\n");
        printf("CREATE STREAM PosDataStream OF PosData;\n");
        break;
      case "TARGET": 
        printf("Example :\n");
        printf("CREATE TARGET SendAlert USING AlertSender(\n        name: unusualActivity\n       ) INPUT FROM AlertStream;\n");
        
        break;
      case "NAMESPACE": 
        printf("Example :\n");
        printf("CREATE NAMESPACE PosApp;\n");
        printf("USE PosApp; // To select namespace\n");
        break;
      case "WINDOW": 
        printf("Example :\n");
        printf("CREATE JUMPING WINDOW PosData5Minutes\nOVER PosDataStream KEEP WITHIN 5 MINUTE ON dateTime\nPARTITION BY merchantId;\n\n");
        
        break;
      case "CACHE": 
        printf("Example :\n");
        printf("CREATE CACHE HourlyAveLookup using CSVReader (\n         directory: '../BloomTests/data',\n         wildcard: 'hourlyData.txt',\n         header: Yes,\n         coldelimiter: ','\n       ) QUERY (keytomap:'merchantId') OF MerchantHourlyAve;\n\n");
        
        break;
      case "CQ": 
        printf("Example :\n");
        printf("CREATE CQ CsvToPosData\nINSERT INTO PosDataStream\nSELECT data[3], stringToDate(data[6],'yyyyMMddHHmmss'),\n      castToInt(substr(data[6],8,10)), \n      castToDouble(data[9]), data[11]\nFROM CsvStream;\n");
        
        break;
      case "APPLICATION": 
        printf("Example :\n");
        printf("CREATE APPLICATION PosApp (\n         SOURCE CsvDataSource,\n         STREAM CsvStream,\n         CQ CsvToPosData);");
        
        printf("start APPLICATION PosApp\n");
        break;
      case "WACTIONSTORE": 
        printf("Example :\n");
        printf("CREATE WACTIONSTORE MerchantActivity\nCONTEXT OF MerchantActivityContext\nEVENT TYPES (\n  MerchantTxRate KEY(merchantId)\n) PERSIST NONE USING ( ) ;\n");
        
        break;
      case "MONITOR": 
      case "MON": 
        CluiMonitorView.printUsage();
        break;
      default: 
        printf("Incorrect object type\n");
      }
    }
  }
  
  private void setBatchmode(String cmd)
  {
    String[] arr = cmd.split(" ");
    if (arr.length <= 2) {
      return;
    }
    this.isBatch = arr[1].equalsIgnoreCase("true");
  }
  
  public static boolean isBalanced(String expression)
  {
    char QUOTE = '"';
    char SINGLEQUOTE = '\'';
    
    Stack<Character> store = new Stack();
    
    boolean failed = false;
    boolean incomment = false;
    for (int i = 0; i < expression.length(); i++) {
      switch (expression.charAt(i))
      {
      case '/': 
        if ((expression.charAt(i + 1) == '*') && (!insinglequote) && (!indoublequote))
        {
          incomment = true;
          i++;
        }
        break;
      case '*': 
        if ((expression.charAt(i + 1) == '/') && (!insinglequote) && (!indoublequote))
        {
          incomment = false;
          i++;
        }
        break;
      case '"': 
        if (((store.isEmpty()) || (((Character)store.peek()).charValue() != '"')) && (!incomment) && (!insinglequote))
        {
          store.push(Character.valueOf('"'));
          indoublequote = true;
        }
        else if ((!store.isEmpty()) && (((Character)store.peek()).charValue() == '"') && (!incomment) && (!insinglequote))
        {
          store.pop();
          indoublequote = false;
        }
        break;
      case '\'': 
        if (((store.isEmpty()) || (((Character)store.peek()).charValue() != '\'')) && (!incomment) && (!indoublequote))
        {
          store.push(Character.valueOf('\''));
          insinglequote = true;
        }
        else if ((!store.isEmpty()) && (((Character)store.peek()).charValue() == '\'') && (!incomment) && (!indoublequote))
        {
          store.pop();
          insinglequote = false;
        }
        break;
      }
    }
    return (store.isEmpty()) && (!failed) && (!incomment);
  }
  
  static void dumpCaches(String cmd)
    throws Exception
  {
    if (!cmd.endsWith(";")) {
      cmd = cmd.substring(0, cmd.length());
    } else {
      cmd = cmd.substring(0, cmd.length() - 1);
    }
    String[] args = cmd.trim().split(" ");
    String cacheName = null;
    boolean detail = false;
    if (args.length >= 2)
    {
      cacheName = args[1];
      if (args.length == 3) {
        detail = "detail".equalsIgnoreCase(args[2]);
      }
    }
    Set<String> cacheNames = PartitionManager.getAllCacheNames();
    if ((cacheName != null) && (!cacheNames.contains(cacheName)))
    {
      printf("The specified WACACHE " + cacheName + " does not exist" + "\n");
      return;
    }
    if (cacheName != null)
    {
      cacheNames.clear();
      cacheNames.add(cacheName);
    }
    if (cacheNames.isEmpty()) {
      printf("No WACACHEs found\n");
    }
    CachingProvider provider = (CachingProvider)Caching.getCachingProvider(CachingProvider.class.getName());
    CacheManager manager = (CacheManager)provider.getCacheManager();
    MutableConfiguration<Object, Object> configuration = new CacheConfiguration();
    configuration.setStoreByValue(false);
    configuration.setStatisticsEnabled(false);
    
    printf("Analyzing the following caches: " + cacheNames + "\n");
    for (String aCacheName : cacheNames)
    {
      ICache cache = (ICache)manager.createCache(aCacheName, configuration);
      manager.startCacheLite(aCacheName);
      Set<ICache.CacheInfo> infos = cache.getCacheInfo();
      manager.destroyCache(aCacheName);
      
      int numServers = 0;
      for (ICache.CacheInfo info : infos)
      {
        MetaInfo.Server s = (MetaInfo.Server)clientOps.getMetaObjectByUUID(info.serverID, WASecurityManager.TOKEN);
        if (s != null) {
          numServers++;
        }
      }
      printf("WACACHE " + aCacheName + " on " + numServers + " servers.\n");
      int numPrimarys = 0;
      int numStale = 0;
      int primarySize = 0;
      int staleSize = 0;
      for (ICache.CacheInfo info : infos)
      {
        MetaInfo.Server s = (MetaInfo.Server)clientOps.getMetaObjectByUUID(info.serverID, WASecurityManager.TOKEN);
        if (s != null)
        {
          printf("  Server " + (s != null ? s.name : info.serverID) + " replication: " + (info.replicationFactor == 0 ? "all" : Integer.valueOf(info.replicationFactor)) + ":");
          printf(" Parts=" + info.numParts + "/" + info.totalSize);
          printf(" Stale=" + info.numStaleParts + "/" + info.totalStaleSize);
          printf(" Indices = " + info.indices);
          printf("\n");
          printf(" \\-Replicas: ");
          for (int i = 0; i < info.replicaParts.length; i++)
          {
            printf("[" + i + "]" + "=" + info.replicaParts[i] + "/" + info.replicaSizes[i] + " ");
            if (i == 0)
            {
              numPrimarys += info.replicaParts[i];
              primarySize += info.replicaSizes[i];
            }
          }
          printf("\n");
          if (detail) {
            printf("    Parts[num][replica] = " + info.parts + "\n");
          }
          numStale += info.numStaleParts;
          staleSize += info.totalStaleSize;
        }
      }
      printf("-->Total:");
      printf(" P=" + numPrimarys + "/" + primarySize);
      printf(" S=" + numStale + "/" + staleSize);
      printf("\n");
    }
  }
  
  public static void describe(String cmd)
    throws MetaDataRepositoryException
  {
    if (!cmd.endsWith(";")) {
      cmd = cmd.trim();
    } else {
      cmd = cmd.substring(0, cmd.length() - 1).trim();
    }
    String[] parts = cmd.trim().split(" ");
    String name = null;
    EntityType type = null;
    if (parts.length == 3)
    {
      name = parts[2];
      String typeVal = parts[1].toUpperCase();
      if (("ABOUT".equals(typeVal)) || ("THIS".equals(typeVal))) {
        typeVal = EntityType.INITIALIZER.toString();
      }
      try
      {
        if (typeVal.equalsIgnoreCase("subscription")) {
          typeVal = "TARGET";
        }
        if ((typeVal.equalsIgnoreCase("namedquery")) || (typeVal.equalsIgnoreCase("query"))) {
          typeVal = "QUERY";
        }
        type = EntityType.valueOf(typeVal);
      }
      catch (IllegalArgumentException e)
      {
        multiPrint("Object type: " + typeVal + " is not a valid type\n");
        return;
      }
    }
    else if (parts.length == 2)
    {
      name = parts[1];
      if (("ABOUT".equals(name.toUpperCase())) || ("THIS".equals(name.toUpperCase()))) {
        type = EntityType.INITIALIZER;
      }
    }
    else
    {
      multiPrint("Command: '" + cmd + "' is invalid. Please use 'describe [type] name'\n");
      return;
    }
    if ((type != null) && (type.equals(EntityType.INITIALIZER)))
    {
      IMap<String, MetaInfo.Initializer> startUpMap = HazelcastSingleton.get().getMap("#startUpMap");
      String clusterName = HazelcastSingleton.getClusterName();
      MetaInfo.Initializer initializer = (MetaInfo.Initializer)startUpMap.get(clusterName);
      multiPrint(initializer.describe(session_id) + "\n");
      return;
    }
    List<MetaInfo.MetaObject> results = new ArrayList();
    for (EntityType aType : EntityType.values()) {
      if ((type == null) || ((type != null) && (aType.equals(type))))
      {
        MetaInfo.MetaObject obj = null;
        if ((aType.equals(EntityType.NAMESPACE)) || (aType.equals(EntityType.SERVER)))
        {
          if (aType.equals(EntityType.SERVER)) {
            obj = MetadataRepository.getINSTANCE().getServer(name, session_id);
          } else {
            obj = clientOps.getMetaObjectByName(aType, "Global", name, null, session_id);
          }
        }
        else if (name.indexOf('.') != -1)
        {
          String ns = name.split("\\.")[0];
          String objectname = name.split("\\.")[1];
          obj = clientOps.getMetaObjectByName(aType, ns, objectname, null, session_id);
        }
        else if (aType.isGlobal())
        {
          obj = clientOps.getMetaObjectByName(aType, "Global", name, null, session_id);
        }
        else
        {
          obj = clientOps.getMetaObjectByName(aType, ctx.getCurNamespace().getName(), name, null, session_id);
        }
        if (obj != null) {
          results.add(obj);
        }
      }
    }
    if (results.size() == 0)
    {
      multiPrint("No " + (type == null ? "objects" : type.name().toLowerCase()) + " " + name + " found.\n");
    }
    else
    {
      if (results.size() > 1) {
        multiPrint("Found " + results.size() + " objects\n");
      }
      for (MetaInfo.MetaObject obj : results) {
        if (!obj.getMetaInfoStatus().isDropped()) {
          multiPrint("\n" + obj.describe(session_id) + "\n");
        }
      }
    }
  }
  
  static void listSessions(String cmd)
    throws MetaDataRepositoryException
  {
    String userName = WASecurityManager.getAutheticatedUserName(ctx.getAuthToken());
    MetaInfo.MetaObject mo = MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.USER, "Global", userName, null, ctx.getAuthToken());
    if ((currUserMetaInfo == null) || (!(mo instanceof MetaInfo.User)))
    {
      printf("Could not find expected current user " + userName + "\n");
      return;
    }
    MetaInfo.User currentUser = (MetaInfo.User)mo;
    if (!currentUser.hasGlobalAdminRole())
    {
      printf("User " + userName + " is not authorized to perform that command\n");
      return;
    }
    IMap<AuthToken, SessionInfo> authTokens = HazelcastSingleton.get().getMap("#authTokens");
    for (AuthToken sessionToken : authTokens.keySet())
    {
      SessionInfo session = (SessionInfo)authTokens.get(sessionToken);
      String isCurrentSession = ctx.getAuthToken().equals(sessionToken) ? "*" : " ";
      Date date = new Date(session.loginTime);
      String month = java.text.DateFormatSymbols.getInstance().getShortMonths()[date.getMonth()];
      String time = new SimpleDateFormat("HH:mm").format(date);
      System.out.printf("%s %-15s %-3s %2d %s %18s %n", new Object[] { isCurrentSession, session.userId, month, Integer.valueOf(date.getDate()), time, session.type });
    }
  }
  
  public static void list(String cmd)
    throws MetaDataRepositoryException
  {
    if (!cmd.endsWith(";")) {
      cmd = cmd.trim();
    } else {
      cmd = cmd.substring(0, cmd.length() - 1).trim();
    }
    String[] ar = cmd.trim().split(" ");
    if (ar.length != 2)
    {
      printf("LIST command requires exactly one parameter\n");
      return;
    }
    if (ar[1].equalsIgnoreCase("OBJECTS"))
    {
      String[] possibleTypes = { "TYPES", "STREAMS", "SOURCES", "TARGETS", "NAMESPACES", "WINDOWS", "EVENTTABLES", "CACHES", "CQS", "APPLICATIONS", "FLOWS", "PROPERTYSETS", "PROPERTYVARIABLES", "WACTIONSTORES", "PROPERTYTEMPLATES", "SUBSCRIPTIONS", "SERVERS", "DGS", "USERS", "ROLES", "NAMEDQUERIES", "PAGES", "DASHBOARDS", "QUERYVISUALIZATIONS" };
      for (String type : possibleTypes) {
        printType(type, null);
      }
    }
    else
    {
      printType(ar[1], cmd);
    }
  }
  
  private static void printType(String type, String cmd)
    throws MetaDataRepositoryException
  {
    String PluralToSing = type.substring(0, type.length() - 1).toUpperCase();
    int i;
    switch (type.toUpperCase())
    {
    case "TYPES": 
      printPretty(Utility.removeInternalApplications(clientOps.getByEntityType(EntityType.TYPE, session_id)), PluralToSing);
      break;
    case "STREAMS": 
      printPretty(Utility.removeInternalApplications(clientOps.getByEntityType(EntityType.STREAM, session_id)), PluralToSing);
      break;
    case "SOURCES": 
      printPretty(clientOps.getByEntityType(EntityType.SOURCE, session_id), PluralToSing);
      break;
    case "TARGETS": 
      printPretty(clientOps.getByEntityType(EntityType.TARGET, session_id), PluralToSing);
      break;
    case "NAMESPACES": 
      printPretty(clientOps.getByEntityType(EntityType.NAMESPACE, session_id), PluralToSing);
      break;
    case "WINDOWS": 
      printPretty(clientOps.getByEntityType(EntityType.WINDOW, session_id), PluralToSing);
      break;
    case "EVENTTABLES": 
    case "CACHES": 
      Set<MetaInfo.MetaObject> result = clientOps.getByEntityType(EntityType.CACHE, session_id);
      for (Iterator<MetaInfo.MetaObject> iterator = result.iterator(); iterator.hasNext();)
      {
        MetaInfo.Cache metaObject = (MetaInfo.Cache)iterator.next();
        if ((type.toUpperCase().equalsIgnoreCase("EVENTTABLES")) && (!metaObject.isEventTable())) {
          iterator.remove();
        }
        if ((type.toUpperCase().equalsIgnoreCase("CACHES")) && (metaObject.isEventTable())) {
          iterator.remove();
        }
      }
      printPretty(result, PluralToSing);
      break;
    case "CQS": 
      printPretty(Utility.removeInternalApplications(clientOps.getByEntityType(EntityType.CQ, session_id)), PluralToSing);
      break;
    case "APPLICATIONS": 
      printPretty(Utility.removeInternalApplications(clientOps.getByEntityType(EntityType.APPLICATION, session_id)), PluralToSing);
      break;
    case "FLOWS": 
      printPretty(clientOps.getByEntityType(EntityType.FLOW, session_id), PluralToSing);
      break;
    case "PROPERTYSETS": 
      printPretty(clientOps.getByEntityType(EntityType.PROPERTYSET, session_id), PluralToSing);
      break;
    case "PROPERTYVARIABLES": 
      printPretty(clientOps.getByEntityType(EntityType.PROPERTYVARIABLE, session_id), PluralToSing);
      break;
    case "WACTIONSTORES": 
      printPretty(clientOps.getByEntityType(EntityType.WACTIONSTORE, session_id), PluralToSing);
      break;
    case "PROPERTYTEMPLATES": 
      printPretty(clientOps.getByEntityType(EntityType.PROPERTYTEMPLATE, session_id), PluralToSing);
      break;
    case "PROPERTYSET": 
      printPretty(clientOps.getByEntityType(EntityType.PROPERTYSET, session_id), PluralToSing);
      break;
    case "PROPERTYVARIABLE": 
      printPretty(clientOps.getByEntityType(EntityType.PROPERTYVARIABLE, session_id), PluralToSing);
      break;
    case "SUBSCRIPTIONS": 
      Set temp = new HashSet();
      Set<?> set = clientOps.getByEntityType(EntityType.TARGET, session_id);
      if (set != null) {
        for (Object obj : set) {
          if (((MetaInfo.Target)obj).isSubscription()) {
            temp.add(obj);
          }
        }
      }
      printPretty(temp, PluralToSing);
      break;
    case "SERVERS": 
      printPretty(clientOps.getByEntityType(EntityType.SERVER, session_id), PluralToSing);
      break;
    case "DGS": 
    case "DEPLOYMENTGROUPS": 
      printPretty(clientOps.getByEntityType(EntityType.DG, session_id), PluralToSing);
      break;
    case "USERS": 
      printPretty(clientOps.getByEntityType(EntityType.USER, session_id), PluralToSing);
      break;
    case "ROLES": 
      printPretty(clientOps.getByEntityType(EntityType.ROLE, session_id), PluralToSing);
      break;
    case "NAMEDQUERIES": 
      printPretty(Utility.removeInternalApplications(clientOps.getByEntityType(EntityType.QUERY, session_id)), "NAMEDQUERY");
      break;
    case "WACACHES": 
      Set<String> cacheNames = PartitionManager.getAllCacheNames();
      if (cacheNames != null)
      {
        i = 1;
        for (String cacheName : cacheNames) {
          printf("WACACHE " + i++ + " =>  " + cacheName + "\n");
        }
      }
      break;
    case "PAGES": 
      printPretty(clientOps.getByEntityType(EntityType.PAGE, session_id), PluralToSing);
      break;
    case "DASHBOARDS": 
      printPretty(clientOps.getByEntityType(EntityType.DASHBOARD, session_id), PluralToSing);
      break;
    case "QUERYVISUALIZATIONS": 
      printPretty(clientOps.getByEntityType(EntityType.QUERYVISUALIZATION, session_id), PluralToSing);
      break;
    case "SESSIONS": 
      listSessions(cmd);
      break;
    default: 
      multiPrint("List command is followed by an object type in plural form. e.g: 'List namespaces'\n");
    }
  }
  
  static void testCmd(String cmd)
    throws Exception
  {
    if (cmd.trim().endsWith(";"))
    {
      cmd = cmd.trim();
      cmd = cmd.substring(0, cmd.length() - 1);
    }
    String[] tu = cmd.split(" ");
    if ((tu.length > 1) && (tu[1].equalsIgnoreCase("compatibility"))) {
      storeCommand(cmd, "testCompatibility");
    } else {
      printf("USAGE : TEST COMPATIBILITY <optional: json file location on server>\n");
    }
  }
  
  static void printPretty(Set<? extends MetaInfo.MetaObject> l, String type)
    throws MetaDataRepositoryException
  {
    int i;
    if (l == null)
    {
      multiPrint("No " + type + " found\n");
    }
    else
    {
      i = 1;
      for (MetaInfo.MetaObject object : l) {
        if ((object instanceof MetaInfo.Namespace))
        {
          multiPrint(type + " " + i++ + " =>  " + object.name + "\n");
        }
        else if ((object instanceof MetaInfo.User))
        {
          multiPrint(type + " " + i++ + " =>  " + object.name + "\n");
        }
        else if ((object instanceof MetaInfo.Role))
        {
          multiPrint(type + " " + i++ + " =>  " + object.nsName + "." + object.name + "\n");
        }
        else if ((object instanceof MetaInfo.Server))
        {
          List<String> dgroups = new ArrayList();
          for (UUID sId : ((MetaInfo.Server)object).deploymentGroupsIDs)
          {
            MetaInfo.DeploymentGroup dg = (MetaInfo.DeploymentGroup)clientOps.getMetaObjectByUUID(sId, session_id);
            if (dg != null) {
              dgroups.add(dg.name);
            } else {
              dgroups.add(sId.getUUIDString());
            }
          }
          multiPrint((((MetaInfo.Server)object).isAgent ? "AGENT" : "SERVER") + " " + i++ + " =>  " + object.name + " [" + object.uuid + "] in " + dgroups + "\n");
        }
        else if ((object instanceof MetaInfo.DeploymentGroup))
        {
          List<String> servers = new ArrayList();
          for (UUID sId : ((MetaInfo.DeploymentGroup)object).groupMembers.keySet())
          {
            MetaInfo.Server s = (MetaInfo.Server)clientOps.getMetaObjectByUUID(sId, session_id);
            if (s != null) {
              servers.add(s.name);
            } else {
              servers.add(sId.getUUIDString());
            }
          }
          List<String> configuredServers = ((MetaInfo.DeploymentGroup)object).configuredMembers;
          multiPrint(type + " " + i++ + " =>  " + object.name + " has actual servers " + servers + " and configured servers " + configuredServers + " with mininum required servers " + ((MetaInfo.DeploymentGroup)object).getMinimumRequiredServers() + "\n");
        }
        else
        {
          multiPrint(type + " " + i++ + " =>  " + object.nsName + "." + object.name + "\n");
        }
      }
    }
  }
  
  static void backup(String cmd)
  {
    if (cmd.endsWith(";")) {
      cmd = cmd.substring(0, cmd.length() - 1);
    }
    String[] ar = cmd.trim().split(" ");
    switch (ar[1].toUpperCase())
    {
    case "LIST": 
      File backupsDir = new File("bin/DerbyBackups");
      if (!backupsDir.exists())
      {
        System.out.println("Backup dir not found: " + backupsDir.getAbsolutePath());
      }
      else
      {
        File[] potentialBackups = backupsDir.listFiles();
        int backupCount = 0;
        for (int i = 0; i < potentialBackups.length; i++)
        {
          File pbu = potentialBackups[i];
          if (pbu.isDirectory()) {
            if (pbu.getName().matches("\\d\\d\\d\\d-\\d\\d-\\d\\d_\\d\\d-\\d\\d-\\d\\d"))
            {
              System.out.println(i + ". " + pbu.getName());
              backupCount++;
            }
          }
        }
        if (backupCount == 0) {
          System.out.println("No backups found in " + backupsDir.getAbsolutePath());
        }
      }
      break;
    case "NOW": 
      try
      {
        Process p = Runtime.getRuntime().exec("bin/backupDerby.sh");
        BufferedReader isr = new BufferedReader(new InputStreamReader(p.getInputStream()));
        BufferedReader esr = new BufferedReader(new InputStreamReader(p.getErrorStream()));
        String iline;
        String eline;
        do
        {
          iline = isr.readLine();
          if (iline != null) {
            System.out.println(iline);
          }
          eline = esr.readLine();
          if (eline != null) {
            System.out.println(eline);
          }
        } while ((iline != null) || (eline != null));
      }
      catch (IOException e)
      {
        logger.error(e);
      }
    }
  }
  
  private String getPrompt()
  {
    String prompt = this.commandLineFormat;
    if (prompt.contains("%A"))
    {
      String ns = ctx == null ? "" : ctx.getCurNamespace().name;
      prompt = prompt.replace("%A", ns);
    }
    if (prompt.contains("%U"))
    {
      String userName = currUserMetaInfo != null ? currUserMetaInfo.getName() : "??";
      
      prompt = prompt.replace("%U", userName);
    }
    if (prompt.contains("%C"))
    {
      String cluster = HazelcastSingleton.get().getName();
      prompt = prompt.replace("%C", cluster);
    }
    return prompt;
  }
  
  public static WAQueue getSessionQueue()
  {
    return sessionQueue;
  }
  
  public static WAQueue.Listener getQueuelistener()
  {
    return queuelistener;
  }
  
  public static void listLogLevels()
  {
    multiPrint("Main LOGGER: " + Logger.getRootLogger().getName() + " " + Logger.getRootLogger().getLevel() + "\n { \n");
    long counter = 0L;
    String format = "\t%s %d %s  %s\t%s%n";
    for (Enumeration<Logger> loggerEnumeratin = LogManager.getCurrentLoggers(); loggerEnumeratin.hasMoreElements();)
    {
      Logger XX = (Logger)loggerEnumeratin.nextElement();
      multiPrint(String.format(format, new Object[] { "LOGGER", Long.valueOf(++counter), "=>", XX.getName(), XX.getLevel() == null ? "Not Set" : XX.getLevel() }));
    }
    multiPrint(" } \n");
  }
}
