package com.bloom.waction;

import com.bloom.anno.EntryPoint;
import com.bloom.event.QueryResultEvent;
import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.persistence.WactionStore;
import com.bloom.proc.events.JsonNodeEvent;
import com.bloom.runtime.DistributedExecutionManager;
import com.bloom.runtime.QueryValidator;
import com.bloom.runtime.Server;
import com.bloom.runtime.containers.TaskEvent;
import com.bloom.runtime.monitor.MonitoringApiQueryHandler;
import com.bloom.security.WASecurityManager;
import com.bloom.uuid.AuthToken;
import com.bloom.uuid.UUID;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.bloom.event.ObjectMapperFactory;
import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.ITaskEvent;
import com.bloom.runtime.containers.WAEvent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.log4j.Logger;

public class WactionApi
{
  private static Logger logger = Logger.getLogger(WactionApi.class);
  public static final String MONITOR_NAME = "WAStatistics";
  private static WactionApi instance = new WactionApi();
  private QueryValidator qVal;
  private QueryRunnerForWactionApi forES;
  
  private static abstract enum WActionKeyFields
  {
    uuid,  key,  timestamp;
    
    private WActionKeyFields() {}
    
    public abstract String getFieldRepresentation();
  }
  
  public static WactionApi get()
  {
    return instance;
  }
  
  private WactionApi()
  {
    this.qVal = new QueryValidator(Server.server);
  }
  
  public String toJSON(Object o)
    throws JsonProcessingException
  {
    ObjectMapper mapper = ObjectMapperFactory.newInstance();
    String result = mapper.writeValueAsString(o);
    return result;
  }
  
  public QueryValidator getqVal()
  {
    return this.qVal;
  }
  
  public void setqVal(QueryValidator qVal)
  {
    this.qVal = qVal;
  }
  
  public QueryRunnerForWactionApi getForES()
  {
    return this.forES;
  }
  
  public void setForES(QueryRunnerForWactionApi forES)
  {
    this.forES = forES;
  }
  
  public Object[] getWactionStoreDef(String storename)
    throws Exception
  {
    return WactionStore.getWactionStoreDef(storename);
  }
  
  public Object get(String storename, WactionKey key, String[] fields, Map<String, Object> filter)
    throws Exception
  {
    return get(storename, key, fields, filter, WASecurityManager.TOKEN);
  }
  
  public Object get(String storename, WactionKey key, String[] fields, Map<String, Object> filter, AuthToken token)
    throws Exception
  {
    HazelcastInstance hz = HazelcastSingleton.get();
    if (filter == null) {
      filter = new HashMap();
    }
    if (!filter.containsKey("singlewactions")) {
      filter.put("singlewactions", "false");
    }
    WActionApiQueryHandler handler = new WActionApiQueryHandler(storename, key, fields, filter, token);
    Object results = null;
    if (HazelcastSingleton.isClientMember())
    {
      Set<Member> srvs = DistributedExecutionManager.getFirstServer(hz);
      if (srvs.isEmpty()) {
        throw new RuntimeException("No available Server to get wactions data");
      }
      Object tR = DistributedExecutionManager.exec(hz, handler, srvs);
      if ((tR instanceof Collection))
      {
        Collection coll = (Collection)tR;
        if (coll.size() != 1) {
          throw new RuntimeException("Expected exactly 1 map(Map<WactionKey, Map<String, Object>>) with All WActions. But got " + coll.size());
        }
        results = coll.iterator().next();
      }
    }
    else
    {
      results = handler.call();
    }
    handler = null;
    return results;
  }
  
  public Object executeQuery(String storename, String queryString, Map<String, Object> queryParams, AuthToken token)
    throws Exception
  {
    HazelcastInstance hz = HazelcastSingleton.get();
    WActionApiQueryHandler handler = new WActionApiQueryHandler(storename, queryString, queryParams, token);
    Object results = null;
    if (HazelcastSingleton.isClientMember())
    {
      Set<Member> srvs = DistributedExecutionManager.getFirstServer(hz);
      if (srvs.isEmpty()) {
        throw new RuntimeException("No available Server to get wactions data");
      }
      Object tR = DistributedExecutionManager.exec(hz, handler, srvs);
      if ((tR instanceof Collection))
      {
        Collection coll = (Collection)tR;
        if (coll.size() != 1) {
          throw new RuntimeException("Expected exactly 1 map(Map<WactionKey, Map<String, Object>>) with All WActions. But got " + coll.size());
        }
        results = coll.iterator().next();
      }
    }
    else
    {
      results = handler.call();
    }
    handler = null;
    return results;
  }
  
  public static Object covertTaskEventsToMap(ITaskEvent taskEvent, String keyField, String[] fields, Map<String, String> ctxFields, boolean isSqlContext)
  {
    long stime = System.currentTimeMillis();
    
    Iterator<WAEvent> eventIterator = taskEvent.batch().iterator();
    List<Map<String, Object>> mMap = new ArrayList();
    StringBuilder builder = new StringBuilder();
    String[] fieldInfo = null;
    while (eventIterator.hasNext())
    {
      WactionKey wKey = new WactionKey();
      Map<String, Object> data = new HashMap();
      QueryResultEvent qE = (QueryResultEvent)((WAEvent)eventIterator.next()).data;
      String[] fieldsInfo = qE.getFieldsInfo();
      if (isSqlContext)
      {
        wKey.setId(qE.getIDString());
        wKey.setWactionKey(qE.getKey());
        for (int ii = 0; ii < fieldsInfo.length; ii++) {
          data.put(fieldsInfo[ii], qE.getPayload()[ii]);
        }
      }
      else if (fieldsInfo[0].startsWith("last"))
      {
        JsonNode node;
        if ((qE.getPayload()[0] instanceof JsonNodeEvent))
        {
          JsonNodeEvent nodeEvent = (JsonNodeEvent)qE.getPayload()[0];
          node = nodeEvent.getData();
          builder.setLength(0);
          for (Map.Entry<String, String> entry : ctxFields.entrySet())
          {
            JsonNode val = node.get((String)entry.getKey());
            if (val != null)
            {
              if ((val instanceof TextNode)) {
                data.put((String)entry.getKey(), val.textValue());
              } else {
                data.put((String)entry.getKey(), val.toString());
              }
              builder.setLength(0);
            }
          }
        }
        else
        {
          if (fieldInfo == null) {
            fieldInfo = extractFieldName(qE.getFieldsInfo());
          }
          builder.setLength(0);
          for (int ii = 0; ii < fieldInfo.length; ii++)
          {
            data.put(fieldInfo[ii], qE.getPayload()[ii]);
            builder.setLength(0);
          }
        }
      }
      else
      {
        builder.setLength(0);
        for (int ii = 0; ii < fieldsInfo.length; ii++)
        {
          data.put(qE.getFieldsInfo()[ii], qE.getPayload()[ii]);
          builder.setLength(0);
        }
      }
      mMap.add(data);
    }
    if (logger.isDebugEnabled())
    {
      logger.debug("TaskEvent size: " + taskEvent.batch().size());
      logger.debug("Result Map size: " + mMap.size());
    }
    long etime = System.currentTimeMillis();
    logger.info("Time to convert data to consumable format: " + (etime - stime) / 1000.0D + " seconds");
    return mMap;
  }
  
  private static String[] extractFieldName(String[] fieldsInfo)
  {
    String[] fields = new String[fieldsInfo.length];
    int ii = 0;
    for (String ss : fieldsInfo)
    {
      int dot = ss.indexOf(".") + 1;
      int cBrac = ss.indexOf(")");
      String actualField = ss.substring(dot, cBrac);
      fields[ii] = actualField;
      ii++;
    }
    return fields;
  }
  
  public static ITaskEvent convertApiResultToEvents(Map<WactionKey, Map<String, Object>> map)
  {
    LinkedList<WAEvent> linkedList = new LinkedList();
    for (Map.Entry<WactionKey, Map<String, Object>> entry : map.entrySet())
    {
      Map<String, Object> entryVal = (Map)entry.getValue();
      QueryResultEvent qE = new QueryResultEvent();
      String timestamp = (String)entryVal.get("timestamp");
      
      qE.setID(new UUID(timestamp));
      
      qE.setTimeStamp(new Long(timestamp).longValue());
      List<String> allFields = new ArrayList();
      List<Object> payload = new ArrayList();
      for (Map.Entry<String, Object> entries : entryVal.entrySet()) {
        if (((String)entries.getKey()).startsWith("context-"))
        {
          String[] fieldInfo = ((String)entries.getKey()).split("-");
          allFields.add(fieldInfo[1]);
          payload.add(entries.getValue());
        }
      }
      qE.setFieldsInfo((String[])allFields.toArray(new String[allFields.size()]));
      qE.setPayload(payload.toArray(new Object[payload.size()]));
      WAEvent waEvent = new WAEvent();
      waEvent.data = qE;
      linkedList.add(waEvent);
    }
    ITaskEvent TE = TaskEvent.createStreamEvent(linkedList);
    return TE;
  }
  
  @EntryPoint(usedBy=1)
  public Object getAllWactions(String storename, String[] fields, Map<String, Object> filter, AuthToken authToken)
    throws Exception
  {
    if (storename.equals("WAStatistics"))
    {
      MonitoringApiQueryHandler handler = new MonitoringApiQueryHandler(storename, null, fields, filter, authToken, true);
      return handler.get();
    }
    return get(storename, null, fields, filter, authToken);
  }
  
  @EntryPoint(usedBy=1)
  public Object getWaction(String storename, WactionKey key, String[] fields, Map<String, Object> filter, AuthToken authToken)
    throws Exception
  {
    if (storename.equals("WAStatistics"))
    {
      MonitoringApiQueryHandler handler = new MonitoringApiQueryHandler(storename, key, fields, filter, authToken, false);
      return handler.get();
    }
    return get(storename, key, fields, filter, authToken);
  }
}
