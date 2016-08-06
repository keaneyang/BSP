package com.bloom.tungsten;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.metaRepository.MDClientOps;
import com.bloom.metaRepository.MDRepository;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.metaRepository.RemoteCall;
import com.bloom.runtime.Context;
import com.bloom.runtime.DistributedExecutionManager;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Flow;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.Server;
import com.bloom.runtime.monitor.MonitorModel;
import com.bloom.runtime.monitor.MonitoringApiQueryHandler;
import com.bloom.uuid.UUID;
import com.bloom.waction.WactionKey;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.log4j.Logger;

public class CluiMonitorView
{
  private static Logger logger = Logger.getLogger(CluiMonitorView.class);
  public static final SimpleDateFormat DATE_FORMAT_MILLIS = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss:SSS");
  public static final String DATE_FORMAT_REGEX_MILLIS = "\\d\\d\\d\\d/\\d\\d/\\d\\d-\\d\\d:\\d\\d:\\d\\d:\\d\\d\\d";
  public static final SimpleDateFormat DATE_FORMAT_SECS = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss");
  public static final String DATE_FORMAT_REGEX_SECS = "\\d\\d\\d\\d/\\d\\d/\\d\\d-\\d\\d:\\d\\d:\\d\\d";
  public static final SimpleDateFormat DATE_FORMAT_MINS = new SimpleDateFormat("yyyy/MM/dd-HH:mm");
  public static final String DATE_FORMAT_REGEX_MINS = "\\d\\d\\d\\d/\\d\\d/\\d\\d-\\d\\d:\\d\\d";
  public static final String DATE_FORMAT_REGEX_HOUR_MIN = "\\d\\d:\\d\\d";
  public static final String DATE_FORMAT_REGEX_ALL_DIGITS = "\\d\\d:\\d\\d";
  public static final int TABLE_WIDTH = 135;
  private static final long FIVE_SECONDS = 5000L;
  private static final String appTableFormat = "= %-42s%-20s%-10s%-20s%-10s%-10s%-20s=%n";
  private static final String singleCompFormat = "= %-72s%-60s=%n";
  private static final String serverTableFormat = "= %-42s%-40s%-20s%-10s%-20s=%n";
  private static final String kafkaTableFormat = "= %-42s%-90s=%n";
  private static final int WORD_WRAP_MAX = 10;
  
  private static MetaInfo.MetaObject getObjectByName(String name)
  {
    for (EntityType aType : )
    {
      String fullName = null;
      if (aType.equals(EntityType.SERVER)) {
        try
        {
          return MetadataRepository.getINSTANCE().getServer(name, Tungsten.session_id);
        }
        catch (MetaDataRepositoryException e)
        {
          e.printStackTrace();
          return null;
        }
      }
      if (name.indexOf('.') != -1) {
        fullName = name;
      } else if (aType.isGlobal()) {
        fullName = "Global." + name;
      } else {
        fullName = Tungsten.ctx.addSchemaPrefix(null, name);
      }
      String[] namespaceAndName = fullName.split("\\.");
      MetaInfo.MetaObject obj = null;
      try
      {
        obj = MetadataRepository.getINSTANCE().getMetaObjectByName(aType, namespaceAndName[0], namespaceAndName[1], null, Tungsten.session_id);
        if (obj != null) {
          return obj;
        }
      }
      catch (MetaDataRepositoryException e)
      {
        e.printStackTrace();
      }
    }
    return null;
  }
  
  public static void handleMonitorRequest(List<String> params)
    throws MetaDataRepositoryException
  {
    MDRepository clientOps = new MDClientOps(HazelcastSingleton.get().getName());
    if (!MonitorModel.monitorIsEnabled())
    {
      System.out.println("Monitor is disabled");
      return;
    }
    LinkedList<String> tokens = new LinkedList(params);
    String token = (String)tokens.pollFirst();
    if ((token != null) && (token.equalsIgnoreCase("help")))
    {
      printUsage();
      return;
    }
    MetaInfo.MetaObject entityInfo = null;
    if (token == null)
    {
      entityInfo = null;
    }
    else
    {
      MetaInfo.MetaObject mo = getObjectByName(token);
      if (mo != null)
      {
        entityInfo = mo;
      }
      else
      {
        entityInfo = null;
        System.out.println("MONITOR could not find application, node or component named " + token);
        return;
      }
    }
    token = (String)tokens.pollFirst();
    MetaInfo.Server serverInfo;
    if (token == null)
    {
      serverInfo = null;
    }
    else
    {
      MetaInfo.MetaObject mo = getObjectByName(token);
      if ((mo instanceof MetaInfo.Server))
      {
        serverInfo = (MetaInfo.Server)mo;
      }
      else
      {
        if (mo != null)
        {
          System.out.println("MONITOR could not find node " + token);
          return;
        }
        serverInfo = null;
        tokens.addFirst(token);
      }
    }
    boolean verbose = false;
    long followTime = -1L;
    Long startTime = null;
    Long endTime = null;
    for (token = (String)tokens.pollFirst(); token != null; token = (String)tokens.pollFirst()) {
      if (token.equals("-v"))
      {
        verbose = true;
      }
      else if (token.equals("-follow"))
      {
        token = (String)tokens.peekFirst();
        if ((token == null) || (token.startsWith("-")))
        {
          followTime = 5000L;
        }
        else
        {
          try
          {
            followTime = Integer.valueOf(token).intValue();
          }
          catch (NumberFormatException e)
          {
            System.out.println("MONITOR -follow argument \"" + followTime + "\" cannot be parsed as an integer");
            return;
          }
          followTime *= 1000L;
          tokens.poll();
        }
      }
      else if (token.equals("-start"))
      {
        token = (String)tokens.peekFirst();
        if ((token == null) || (token.startsWith("-")))
        {
          System.out.println("MONITOR -start requires an argument");
          return;
        }
        startTime = convertToTimestamp(token);
        if (startTime == null)
        {
          System.out.println("MONITOR -start requires an argument in the format " + DATE_FORMAT_MINS.toPattern());
          return;
        }
        tokens.poll();
      }
      else if (token.equals("-end"))
      {
        token = (String)tokens.pollFirst();
        if ((token == null) || (token.startsWith("-")))
        {
          System.out.println("MONITOR -end requires an argument");
          return;
        }
        endTime = convertToTimestamp(token);
        if (endTime == null)
        {
          System.out.println("MONITOR -end requires an argument in the format " + DATE_FORMAT_MINS.toPattern());
          return;
        }
        tokens.poll();
      }
      else
      {
        System.out.println("MONITOR syntax error reading " + token);
        printUsage();
        return;
      }
    }
    executeCommand(entityInfo, serverInfo, verbose, followTime, startTime, endTime);
  }
  
  private static void printBasicMonitorStatus(boolean verbose, Long startTime, Long endTime)
    throws MetaDataRepositoryException
  {
    MDRepository clientOps = new MDClientOps(HazelcastSingleton.get().getName());
    
    Set<MetaInfo.MetaObject> mos = clientOps.getByEntityType(EntityType.APPLICATION, Tungsten.session_id);
    List<MetaInfo.Flow> flowMOs = new ArrayList();
    for (MetaInfo.MetaObject mo : mos) {
      if ((mo instanceof MetaInfo.Flow))
      {
        MetaInfo.Flow mif = (MetaInfo.Flow)mo;
        flowMOs.add((MetaInfo.Flow)mo);
      }
    }
    printApplications(flowMOs, null, "APPLICATIONS", verbose, startTime, endTime);
    
    mos = clientOps.getByEntityType(EntityType.SERVER, Tungsten.session_id);
    List<MetaInfo.Server> serverMOs = new ArrayList();
    for (MetaInfo.MetaObject mo : mos) {
      if ((mo instanceof MetaInfo.Server)) {
        serverMOs.add((MetaInfo.Server)mo);
      }
    }
    printServers(serverMOs, "NODES", verbose, startTime, endTime);
    
    printKafka();
    
    printElasticsearch();
  }
  
  private static void printKafka()
  {
    String[] fields = new String[0];
    Map<String, Object> filter = new HashMap();
    filter.put("serverInfo", MonitorModel.KAFKA_SERVER_UUID);
    
    Map<String, Object> summary = getEntitySummary(MonitorModel.KAFKA_ENTITY_UUID, fields, filter);
    
    printTableTitle("KAFKA");
    if ((summary == null) || (summary.isEmpty()))
    {
      printTableMessage("No Kafka-based persistent streams in use");
    }
    else
    {
      Object[] columnNames = { "Name", "Values" };
      System.out.format("= %-42s%-90s=%n", columnNames);
      printTableDivider();
      for (String name : summary.keySet())
      {
        Object value = summary.get(name);
        Object[] printVals = humanizeData(name, value);
        Object[] lineData = { printVals[0], printVals[1] };
        printLine("= %-42s%-90s=%n", lineData, 0);
      }
    }
    printTableBottom();
  }
  
  private static void printElasticsearch()
  {
    String[] fields = new String[0];
    Map<String, Object> filter = new HashMap();
    
    HazelcastInstance hz = HazelcastSingleton.get();
    Set<Member> srvs = DistributedExecutionManager.getFirstServer(hz);
    if (srvs.isEmpty()) {
      throw new RuntimeException("No available Node to get Monitor data");
    }
    WactionKey wk = new WactionKey(MonitorModel.ES_ENTITY_UUID, MonitorModel.ES_SERVER_UUID);
    GetMonWaction action = new GetMonWaction(wk, fields, filter);
    Map<WactionKey, Map<String, Object>> x;
    Map<String, Object> summary;
    printTableTitle("ELASTICSEARCH");
    try
    {
      Collection<Map<WactionKey, Map<String, Object>>> manyResults = DistributedExecutionManager.exec(hz, action, srvs);
      if (manyResults.size() != 1) {
        logger.warn("Expected result from exactly one node, but received " + manyResults.size());
      }
      x = (Map)manyResults.iterator().next();
      
      Object[] columnNames = { "Name", "Values" };
      System.out.format("= %-42s%-90s=%n", columnNames);
      printTableDivider();
      for (WactionKey key : x.keySet())
      {
        summary = (Map)x.get(key);
        for (String name : summary.keySet())
        {
          Object value = summary.get(name);
          Object[] printVals = humanizeData(name, value);
          Object[] lineData = { printVals[0], printVals[1] };
          printLine("= %-42s%-90s=%n", lineData, 0);
        }
      }
    }
    catch (Exception e)
    {
     
      System.out.println("Could not get Elasticsearch data: " + e.getMessage());
    }
    printTableBottom();
  }
  
  private static void printServer(MetaInfo.Server serverInfo, Long startTime, Long endTime)
    throws MetaDataRepositoryException
  {
    List<MetaInfo.Server> serverInfos = new ArrayList();
    serverInfos.add(serverInfo);
    printServers(serverInfos, "NODE " + serverInfo.name, true, startTime, endTime);
  }
  
  private static void printServers(List<MetaInfo.Server> serverInfos, String tableTitle, boolean recurse, Long startTime, Long endTime)
    throws MetaDataRepositoryException
  {
    printTableTitle(tableTitle);
    if (serverInfos.size() > 0)
    {
      Object[] columnNames = { "Name", "Version", "Free Mem", "CPU%", "Uptime" };
      System.out.format("= %-42s%-40s%-20s%-10s%-20s=%n", columnNames);
      printTableDivider();
      for (MetaInfo.Server serverInfo : serverInfos) {
        printServerTree(serverInfo, recurse, startTime, endTime, 0);
      }
    }
    else
    {
      printTableMessage("No available nodes");
    }
    printTableBottom();
  }
  
  private static void printServerTree(MetaInfo.MetaObject metaObject, boolean recurse, Long startTime, Long endTime, int depth)
    throws MetaDataRepositoryException
  {
    String[] fields = new String[0];
    Map<String, Object> filter = new HashMap();
    if (startTime != null) {
      filter.put("start-time", startTime);
    }
    if (endTime != null) {
      filter.put("end-time", endTime);
    }
    Map<String, Object> values = getEntitySummary(metaObject.uuid, fields, filter);
    if (values == null)
    {
      System.out.println("= " + metaObject.name + " - data not ready yet");
      return;
    }
    Object name = EntityType.SERVER.equals(metaObject.type) ? getOneValue("name", values, "?") : getOneValue("full-name", values, "?");
    Object version = getOneValue("version", values, ".");
    Object memFree = getOneValue("memory-free", values, ".");
    Object uptime = getOneValue("uptime", values, ".");
    
    String uptimeStr = (uptime instanceof Long) ? getUptimeString(((Long)uptime).longValue()) : uptime.toString();
    String versionStr = version.toString().replace("Version ", "").replace("1.0.0-SNAPSHOT ", "");
    String freeMemStr = (memFree instanceof Long) ? getMemoryString(((Long)memFree).longValue()) : memFree.toString();
    
    Object cpu = getOneValue("cpu-rate", values, ".");
    if ((cpu instanceof Long)) {
      cpu = MonitorModel.renderCpuPercent(((Long)cpu).longValue());
    }
    Object[] lineData = { name, versionStr, freeMemStr, cpu, uptimeStr };
    printLine("= %-42s%-40s%-20s%-10s%-20s=%n", lineData, depth);
    MDRepository clientOps = new MDClientOps(HazelcastSingleton.get().getName());
    Map<String, List<MetaInfo.MetaObject>> m;
    if (recurse)
    {
      MetaInfo.Flow flowMo;
      if ((metaObject instanceof MetaInfo.Flow))
      {
        flowMo = (MetaInfo.Flow)metaObject;
        for (EntityType et : flowMo.getObjectTypes()) {
          for (UUID u : flowMo.getObjects(et))
          {
            MetaInfo.MetaObject childMO = clientOps.getMetaObjectByUUID(u, Tungsten.session_id);
            printServerTree(childMO, recurse, startTime, endTime, depth + 1);
          }
        }
      }
      else if ((metaObject instanceof MetaInfo.Server))
      {
        MetaInfo.Server serverMo = (MetaInfo.Server)metaObject;
        m = serverMo.getCurrentObjects();
        for (String keyStr : m.keySet())
        {
          List<MetaInfo.MetaObject> mos = (List)m.get(keyStr);
          for (MetaInfo.MetaObject childMO : mos) {
            if ((childMO instanceof MetaInfo.Flow)) {
              printServerTree(childMO, recurse, startTime, endTime, depth + 1);
            }
          }
        }
      }
    }
  }
  
  private static void printApplication(MetaInfo.Flow applicationInfo, MetaInfo.Server serverInfo, boolean verbose, Long startTime, Long endTime)
    throws MetaDataRepositoryException
  {
    List<MetaInfo.Flow> flowInfos = new ArrayList();
    flowInfos.add(applicationInfo);
    printApplications(flowInfos, serverInfo, EntityType.APPLICATION.equals(applicationInfo.type) ? "APPLICATION" : "FLOW", verbose, startTime, endTime);
  }
  
  private static void printApplications(List<MetaInfo.Flow> flowInfos, MetaInfo.Server serverInfo, String tableTitle, boolean recurse, Long startTime, Long endTime)
    throws MetaDataRepositoryException
  {
    printTableTitle(tableTitle);
    if (flowInfos.size() > 0)
    {
      Object[] columnNames = { "Name", "Status", "Rate", "SourceRate", "CPU%", "Nodes", "Activity" };
      System.out.format("= %-42s%-20s%-10s%-20s%-10s%-10s%-20s=%n", columnNames);
      printTableDivider();
      for (MetaInfo.MetaObject flowInfo : flowInfos) {
        printApplicationTree(flowInfo, serverInfo, recurse, startTime, endTime, 0);
      }
    }
    else
    {
      printTableMessage("No applications");
    }
    printTableBottom();
  }
  
  private static void printApplicationTree(MetaInfo.MetaObject metaObject, MetaInfo.Server serverInfo, boolean recurse, Long startTime, Long endTime, int depth)
    throws MetaDataRepositoryException
  {
    String[] fields = new String[0];
    Map<String, Object> filter = new HashMap();
    if (serverInfo != null) {
      filter.put("serverInfo", serverInfo.uuid);
    }
    if (startTime != null) {
      filter.put("start-time", startTime);
    }
    if (endTime != null) {
      filter.put("end-time", endTime);
    }
    Map<String, Object> values = getEntitySummary(metaObject.uuid, fields, filter);
    if (values == null)
    {
      System.out.println("= " + metaObject.name + " - data not ready yet");
      return;
    }
    String name = getOneValue("full-name", values, "?").toString();
    if (name.length() > 42) {
      name = name.substring(0, 20) + "~" + name.substring(name.length() - 20, name.length());
    }
    Object status = getOneValue("status", values, "-");
    if ((status != null) && ((status instanceof String)))
    {
      String statval = (String)status;
      int pos = statval.lastIndexOf(":");
      if (pos != -1) {
        status = statval.substring(pos + 1);
      }
    }
    else
    {
      status = "-";
    }
    Object rate = getOneValue("rate", values, ".");
    Object sourceRate = getOneValue("source-rate", values, "");
    Object cpu = getOneValue("cpu-rate", values, ".");
    if ((cpu instanceof Long)) {
      cpu = MonitorModel.renderCpuPercent(((Long)cpu).longValue());
    }
    Object nodes = getOneValue("num-servers", values, ".");
    if (nodes == null) {
      nodes = ".";
    }
    Object activityVal = getOneValue("latest-activity", values, null);
    String activity = activityVal == null ? "" : DATE_FORMAT_SECS.format(new Date(((Long)activityVal).longValue()));
    Object[] lineData = { name, status, rate, sourceRate, cpu, nodes, activity };
    printLine("= %-42s%-20s%-10s%-20s%-10s%-10s%-20s=%n", lineData, depth);
    MDRepository clientOps = new MDClientOps(HazelcastSingleton.get().getName());
    Map<String, List<MetaInfo.MetaObject>> m;
    if (recurse)
    {
      MetaInfo.Flow flowMo;
      if ((metaObject instanceof MetaInfo.Flow))
      {
        flowMo = (MetaInfo.Flow)metaObject;
        for (EntityType et : flowMo.getObjectTypes()) {
          for (UUID u : flowMo.getObjects(et))
          {
            MetaInfo.MetaObject childMO = clientOps.getMetaObjectByUUID(u, Tungsten.session_id);
            printApplicationTree(childMO, serverInfo, recurse, startTime, endTime, depth + 1);
          }
        }
      }
      else if ((metaObject instanceof MetaInfo.Server))
      {
        MetaInfo.Server serverMo = (MetaInfo.Server)metaObject;
        m = serverMo.getCurrentObjects();
        for (String keyStr : m.keySet())
        {
          List<MetaInfo.MetaObject> mos = (List)m.get(keyStr);
          for (MetaInfo.MetaObject childMO : mos) {
            printApplicationTree(childMO, serverInfo, recurse, startTime, endTime, depth + 1);
          }
        }
      }
    }
  }
  
  private static void printComponent(MetaInfo.MetaObject componentInfo, MetaInfo.Server serverInfo, Boolean recurse, Long startTime, Long endTime)
  {
    String[] fields = new String[0];
    Map<String, Object> filter = new HashMap();
    if (serverInfo != null) {
      filter.put("serverInfo", serverInfo.uuid);
    }
    if (startTime != null) {
      filter.put("start-time", startTime);
    }
    if (endTime != null) {
      filter.put("end-time", endTime);
    }
    Map<String, Object> values = getEntitySummary(componentInfo.uuid, fields, filter);
    if (values == null)
    {
      System.out.println("= " + componentInfo.name + " - data not ready yet");
      return;
    }
    printTableTitle(componentInfo.getFullName());
    Object[] columnNames = { "Property", "Value" };
    System.out.format("= %-72s%-60s=%n", columnNames);
    printTableDivider();
    
    Object nodes = getOneValue("num-servers", values, ".");
    Object[] lineData = { "Nodes", nodes };
    printLine("= %-72s%-60s=%n", lineData, 0);
    Map<String, Object> mostRecent = (Map)values.get("most-recent-data");
    if (mostRecent != null)
    {
      String comments = null;
      for (Map.Entry<String, Object> entry : mostRecent.entrySet()) {
        if (!((String)entry.getKey()).equals("cpu-thread")) {
          if (((String)entry.getKey()).equals("comments"))
          {
            comments = entry.getValue().toString();
          }
          else
          {
            Object[] humanizedData = humanizeData((String)entry.getKey(), entry.getValue());
            printLine("= %-72s%-60s=%n", humanizedData, 0);
          }
        }
      }
      if (comments != null)
      {
        printTableDivider();
        printTableMessage("Comments:");
        String[] lines = comments.split("\n");
        for (String line : lines) {
          printTableMessage(line);
        }
      }
    }
    printTableBottom();
  }
  
  private static Object[] humanizeData(String key, Object value)
  {
    if (key == null) {
      key = "?";
    }
    if (value == null) {
      value = "-";
    }
    if (key.equals("nodes"))
    {
      key = "Nodes";
    }
    else if (key.equals("cpu-thread"))
    {
      key = "CPU% (Thread)";
    }
    else if (key.equals("cpu-rate"))
    {
      key = "CPU Rate";
    }
    else if (key.equals("input"))
    {
      key = "Input";
    }
    else if (key.equals("rate"))
    {
      key = "Rate";
    }
    else if (key.equals("cpu"))
    {
      key = "CPU%";
    }
    else if (key.equals("input-rate"))
    {
      key = "Input Rate";
    }
    else if (key.equals("cpu-time"))
    {
      key = "CPU Time";
      if ((value instanceof Long))
      {
        Double doubleValue = Double.valueOf(((Long)value).longValue() / 1.0E9D);
        value = doubleValue + " Seconds";
      }
    }
    else if (key.equals("timestamp"))
    {
      key = "Timestamp";
      if ((value instanceof Long)) {
        value = DATE_FORMAT_MINS.format(new Date(((Long)value).longValue()));
      }
    }
    else if (key.equals("cache-refresh"))
    {
      key = "Cache Refresh";
      if ((value instanceof Long)) {
        value = DATE_FORMAT_MINS.format(new Date(((Long)value).longValue()));
      }
    }
    else if (key.equals("latest-activity"))
    {
      key = "Latest Activity";
      if ((value instanceof Long)) {
        value = DATE_FORMAT_MINS.format(new Date(((Long)value).longValue()));
      }
    }
    else if (key.equals("stream-full"))
    {
      key = "Stream Full";
      if ((value instanceof Long)) {
        value = DATE_FORMAT_MINS.format(new Date(((Long)value).longValue()));
      }
    }
    else if (key.equals("es-tx-bytes"))
    {
      key = "Elasticsearch Transmit Throughput";
      if ((value instanceof Long)) {
        value = getMemoryString(((Long)value).longValue());
      }
    }
    else if (key.equals("es-rx-bytes"))
    {
      key = "Elasticsearch Receive Throughput";
      if ((value instanceof Long)) {
        value = getMemoryString(((Long)value).longValue());
      }
    }
    else if (key.equals("es-total-bytes"))
    {
      key = "Elasticsearch Cluster Storage Total";
      if ((value instanceof Long)) {
        value = getMemoryString(((Long)value).longValue());
      }
    }
    else if (key.equals("es-free-bytes"))
    {
      key = "Elasticsearch Cluster Storage Free";
      if ((value instanceof Long)) {
        value = getMemoryString(((Long)value).longValue());
      }
    }
    else if (key.equals("kafka-bytes-rate"))
    {
      key = "Kafka Writes (Bytes per second)";
      if ((value instanceof Long)) {
        value = getMemoryString(((Long)value).longValue());
      }
    }
    else if (key.equals("kafka-msgs-rate"))
    {
      key = "Kafka Writes (Messages per second)";
      if ((value instanceof Long)) {
        value = getMemoryString(((Long)value).longValue());
      }
    }
    else
    {
      String[] ss = key.split("[-\\s]");
      StringBuilder result = new StringBuilder();
      for (String z : ss)
      {
        if (z.equals("cpu")) {
          result.append("CPU");
        } else {
          result.append(Character.toUpperCase(z.charAt(0)) + z.substring(1));
        }
        result.append(" ");
      }
      key = result.toString();
    }
    return new Object[] { key, value };
  }
  
  private static Object getOneValue(String name, Map<String, Object> values, Object defaultValue)
  {
    Object value = defaultValue;
    if (values.containsKey(name))
    {
      value = values.get(name);
    }
    else if (values.containsKey("most-recent-data"))
    {
      Map<String, Object> mostRecent = (Map)values.get("most-recent-data");
      if (mostRecent.containsKey(name)) {
        value = mostRecent.get(name);
      }
    }
    return value;
  }
  
  public static void printLine(String lineFormat, Object[] lineData, int depth)
  {
    StringBuilder pad = new StringBuilder();
    for (int i = 0; i < depth; i++) {
      pad.append("  ");
    }
    lineData[0] = (pad.toString() + lineData[0]);
    
    System.out.format(lineFormat, lineData);
  }
  
  public static void printUsage()
  {
    System.out.println("usage: mon[itor] [component|application|node [node]] [-follow [<seconds>]] [-end <datetime>]\n\nThe bare 'monitor' command prints a synopsis of the Nodes in the cluster and the Applications loaded. For all monitor commands, 'mon' is also accepted.\n\nIf one parameter is given, it prints a detailed table of that thing. If the parameter is a component it prints a one-line synopsis for that component. If the parameter is an application or a node it prints a table of data including the components belonging to that application or node.\n\nA second parameter is for situations when a component is distributed to multiple nodes. The second parameter indicates the node and the data shown will only be for that component on that node.\n\nThe '-follow' flag will cause the data to be automatically reprinted periodically. The default period is five seconds.\n\nThe '-end' flag requires a timestamp in the format yyyy/MM/dd-HH:mm:ss and will show the most recent available data before that timestamp.");
  }
  
  private static String getMemoryString(long memoryLong)
  {
    if (memoryLong < 0L) {
      return "?";
    }
    double memoryDouble = memoryLong;
    
    String memoryUnit = "b";
    if (memoryDouble > 1024.0D)
    {
      memoryUnit = "Kb";
      memoryDouble /= 1024.0D;
      if (memoryDouble > 1024.0D)
      {
        memoryUnit = "Mb";
        memoryDouble /= 1024.0D;
        if (memoryDouble > 1024.0D)
        {
          memoryUnit = "Gb";
          memoryDouble /= 1024.0D;
        }
      }
    }
    String memoryString = String.format("%.2f", new Object[] { Double.valueOf(memoryDouble) });
    String result = memoryString + memoryUnit;
    return result;
  }
  
  private static String getUptimeString(long uptime)
  {
    if (uptime < 0L) {
      return "?";
    }
    long remainder = uptime;
    long uptimeHours = remainder / 3600000L;
    remainder %= 3600000L;
    long uptimeMinutes = remainder / 60000L;
    remainder %= 60000L;
    long uptimeSeconds = remainder / 1000L;
    remainder %= 1000L;
    String uptimeStr = String.format("%02d:%02d:%02d", new Object[] { Long.valueOf(uptimeHours), Long.valueOf(uptimeMinutes), Long.valueOf(uptimeSeconds) });
    return uptimeStr;
  }
  
  private static void printTableBottom()
  {
    for (int i = 0; i < 135; i++) {
      System.out.print("=");
    }
    System.out.println("");
  }
  
  static void printTableDivider()
  {
    System.out.print("= ");
    for (int i = 0; i < 131; i++) {
      System.out.print("-");
    }
    System.out.print(" =");
    System.out.println("");
  }
  
  public static void printTableTitle(String title)
  {
    System.out.print("============ " + title + " ");
    for (int i = 0; i < 121 - title.length(); i++) {
      System.out.print("=");
    }
    System.out.println("");
  }
  
  private static void printTableMessage(String message)
  {
    int maxLineLength = 131;
    
    StringBuilder result = new StringBuilder();
    int startOffset = 0;
    do
    {
      int endOffset;
      int spacePadding;
      if (message.length() - startOffset < maxLineLength)
      {
         endOffset = message.length();
        spacePadding = maxLineLength - (endOffset - startOffset);
      }
      else
      {
        endOffset = startOffset + maxLineLength;
        spacePadding = 0;
        for (int spaceOffset = 0; spaceOffset < 10; spaceOffset++) {
          if (message.charAt(endOffset - spaceOffset) == ' ')
          {
            spacePadding = spaceOffset;
            endOffset -= spacePadding;
            break;
          }
        }
      }
      result.append("= ");
      result.append(message.substring(startOffset, endOffset));
      for (int i = 0; i < spacePadding; i++) {
        result.append(" ");
      }
      result.append(" =\n");
      
      startOffset = endOffset;
    } while (startOffset < message.length());
    System.out.print(result.toString());
  }
  
  public static class GetMonWaction
    implements RemoteCall<Map<WactionKey, Map<String, Object>>>
  {
    private static final long serialVersionUID = 8814117252476324531L;
    private final WactionKey wk;
    private final String[] fields;
    private final Map<String, Object> filter;
    
    public GetMonWaction(WactionKey wk, String[] fields, Map<String, Object> filter)
    {
      this.wk = wk;
      this.fields = ((String[])fields.clone());
      this.filter = filter;
    }
    
    public Map<WactionKey, Map<String, Object>> call()
      throws Exception
    {
      MonitoringApiQueryHandler handler = new MonitoringApiQueryHandler("WAStatistics", this.wk, this.fields, this.filter, null, false);
      return (Map)handler.get();
    }
  }
  
  private static Map<String, Object> getEntitySummary(UUID objectUuid, String[] fields, Map<String, Object> filter)
  {
    HazelcastInstance hz = HazelcastSingleton.get();
    Set<Member> srvs = DistributedExecutionManager.getFirstServer(hz);
    if (srvs.isEmpty()) {
      throw new RuntimeException("No available Node to get Monitor data");
    }
    WactionKey wk = new WactionKey(objectUuid, null);
    GetMonWaction action = new GetMonWaction(wk, fields, filter);
    try
    {
      Collection<Map<WactionKey, Map<String, Object>>> manyResults = DistributedExecutionManager.exec(hz, action, srvs);
      if (manyResults.size() != 1)
      {
        logger.warn("Expected result from exactly one node, but received " + manyResults.size());
        return null;
      }
      Map<WactionKey, Map<String, Object>> x = (Map)manyResults.iterator().next();
      if (x.size() != 1)
      {
        logger.warn("Expected exactly 1 Waction result, but found " + x.size());
        return null;
      }
      return (Map)x.values().iterator().next();
    }
    catch (Exception e)
    {
      logger.error(e);
    }
    return null;
  }
  
  private static Long convertToTimestamp(String timestampString)
  {
    try
    {
      if (timestampString.matches("\\d\\d\\d\\d/\\d\\d/\\d\\d-\\d\\d:\\d\\d:\\d\\d:\\d\\d\\d")) {
        return Long.valueOf(DATE_FORMAT_MILLIS.parse(timestampString).getTime());
      }
      if (timestampString.matches("\\d\\d\\d\\d/\\d\\d/\\d\\d-\\d\\d:\\d\\d:\\d\\d")) {
        return Long.valueOf(DATE_FORMAT_SECS.parse(timestampString).getTime());
      }
      if (timestampString.matches("\\d\\d\\d\\d/\\d\\d/\\d\\d-\\d\\d:\\d\\d")) {
        return Long.valueOf(DATE_FORMAT_MINS.parse(timestampString).getTime());
      }
      if (timestampString.matches("\\d\\d:\\d\\d"))
      {
        Calendar c = Calendar.getInstance();
        int year = c.get(1);
        int month = c.get(2);
        int day = c.get(5);
        String fullTimestampString = String.format("%04d/%02d/%02d-%s", new Object[] { Integer.valueOf(year), Integer.valueOf(month), Integer.valueOf(day), timestampString });
        return Long.valueOf(DATE_FORMAT_MINS.parse(fullTimestampString).getTime());
      }
      if (timestampString.matches("\\d\\d:\\d\\d")) {
        return Long.valueOf(timestampString);
      }
    }
    catch (ParseException e)
    {
      logger.error("Parse exception getting ", e);
    }
    return null;
  }
  
  private static void executeCommand(MetaInfo.MetaObject entityInfo, MetaInfo.Server serverInfo, boolean verbose, long followTime, Long startTime, Long endTime)
    throws MetaDataRepositoryException
  {
    try
    {
      boolean keepFollowing = followTime >= 0L;
      BufferedReader IO = new BufferedReader(new InputStreamReader(System.in));
      if (keepFollowing) {
        System.out.println("Press any key to stop following");
      }
      do
      {
        long loopStartTime = System.currentTimeMillis();
        if (entityInfo == null)
        {
          printBasicMonitorStatus(verbose, startTime, endTime);
        }
        else if (((entityInfo instanceof MetaInfo.Flow)) && (serverInfo == null))
        {
          MetaInfo.Flow fo = (MetaInfo.Flow)entityInfo;
          printApplication(fo, serverInfo, true, startTime, endTime);
        }
        else if (((entityInfo instanceof MetaInfo.Server)) && (serverInfo == null))
        {
          MetaInfo.Server so = (MetaInfo.Server)entityInfo;
          printServer(so, startTime, endTime);
        }
        else
        {
          printComponent(entityInfo, serverInfo, Boolean.valueOf(true), startTime, endTime);
        }
        while ((keepFollowing) && (System.currentTimeMillis() - loopStartTime < followTime))
        {
          if (IO.ready())
          {
            IO.read();
            keepFollowing = false;
            break;
          }
          Thread.sleep(20L);
        }
      } while (keepFollowing);
    }
    catch (IOException e)
    {
      logger.error("Error while following entity", e);
    }
    catch (InterruptedException e)
    {
      logger.error("Follow-thread interrupted", e);
    }
  }
}
