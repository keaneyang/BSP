package com.bloom.rest;

import com.bloom.exception.InvalidUriException;
import com.bloom.health.HealthMonitor;
import com.bloom.health.HealthMonitorImpl;
import com.bloom.health.HealthRecordCollection;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.User;
import com.bloom.security.WASecurityManager;
import com.bloom.uuid.AuthToken;
import com.bloom.uuid.UUID;
import com.bloom.wactionstore.WAction;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.bloom.event.ObjectMapperFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchPhaseExecutionException;

public class HealthServlet
  extends BloomServlet
{
  private static Logger logger = Logger.getLogger(HealthServlet.class);
  private HealthMonitor healthMonitor;
  ObjectMapper objectMapper = ObjectMapperFactory.newInstance();
  WASecurityManager securityManager = WASecurityManager.get();
  
  public HealthServlet()
  {
    this.healthMonitor = new HealthMonitorImpl();
  }
  
  private static enum Suburi
  {
    start("start"),  end("end"),  size("size"),  from("from"),  clustersize("clusterSize"),  servers("serverHealthMap"),  apps("appHealthMap"),  derby("derbyAlive"),  es("elasticSearch"),  agents("agentCount"),  sources("sourceHealthMap"),  wastores("waStoreHealthMap"),  targets("targetHealthMap"),  caches("cacheHealthMap"),  statechanges("stateChangeList"),  issues("issuesList");
    
    private final String hrFieldName;
    
    private Suburi(String hrFieldName)
    {
      this.hrFieldName = hrFieldName;
    }
    
    public String getHrFieldName()
    {
      return this.hrFieldName;
    }
  }
  
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException
  {
    int responseStatus = 200;
    String responseData = "";
    HealthRecordCollection healthRecordCollection = null;
    Object partialHealthData = null;
    boolean requestedPartialHealthData = false;
    String path = request.getPathInfo();
    Map<String, String[]> parameterMap = new HashMap(request.getParameterMap());
    try
    {
      AuthToken token = getToken(request);
      MetaInfo.User user = this.securityManager.getAuthenticatedUser(token);
      if (user == null) {
        throw new SecurityException("No valid User found for token: " + token);
      }
      logger.info("Request for health data received from user: " + user.getFullName());
      
      parameterMap.remove("token");
      if (parameterMap.size() > 0)
      {
        healthRecordCollection = processRequestByParameters(parameterMap);
      }
      else if ((path == null) || (path.equals("/")))
      {
        healthRecordCollection = filterHealthRecordsByCount(null, null);
      }
      else if (path.startsWith("/"))
      {
        String[] subParts = path.split("/");
        if (subParts.length == 2)
        {
          UUID uuid = new UUID(subParts[1]);
          healthRecordCollection = this.healthMonitor.getHealtRecordById(uuid);
        }
        else if (subParts.length > 2)
        {
          UUID uuid = new UUID(subParts[1]);
          healthRecordCollection = this.healthMonitor.getHealtRecordById(uuid);
          partialHealthData = extractRequestedField(healthRecordCollection, subParts, uuid);
          requestedPartialHealthData = true;
        }
      }
      if (requestedPartialHealthData)
      {
        responseData = this.objectMapper.writeValueAsString(partialHealthData);
        writeResult(responseData, response, responseStatus);
      }
      else if ((healthRecordCollection != null) && (!requestedPartialHealthData))
      {
        responseData = ObjectMapperFactory.newInstance().writeValueAsString(healthRecordCollection);
        writeResult(responseData, response, responseStatus);
      }
      else
      {
        writeResult(responseData, response, responseStatus);
      }
    }
    catch (SearchPhaseExecutionException spee)
    {
      writeException(spee, response, 204);
    }
    catch (JsonProcessingException jpe)
    {
      writeException(jpe, response, 400);
    }
    catch (SecurityException se)
    {
      writeException(se, response, 401);
    }
    catch (InvalidUriException iue)
    {
      writeException(iue, response, 400);
    }
    catch (Exception e)
    {
      writeException(e, response, 400);
    }
  }
  
  private HealthRecordCollection processRequestByParameters(Map<String, String[]> parameterMap)
    throws Exception
  {
    HealthRecordCollection healthRecordCollection = null;
    if (parameterMap.get(String.valueOf(Suburi.size)) != null) {
      healthRecordCollection = filterHealthRecordsByCount((String[])parameterMap.get(String.valueOf(Suburi.size)), (String[])parameterMap.get(String.valueOf(Suburi.from)));
    } else if (parameterMap.get(String.valueOf(Suburi.start)) != null) {
      healthRecordCollection = filterHealthRecordsByTime((String[])parameterMap.get(String.valueOf(Suburi.start)), (String[])parameterMap.get(String.valueOf(Suburi.end)));
    } else {
      throw new Exception("Invalid URI for request");
    }
    return healthRecordCollection;
  }
  
  private HealthRecordCollection filterHealthRecordsByTime(String[] strings0, String[] strings1)
    throws Exception
  {
    long startTime = Long.parseLong(strings0[0]);
    long endTime = -1L;
    if ((strings1 != null) && (strings1[0] != null)) {
      endTime = Long.parseLong(strings1[0]);
    }
    HealthRecordCollection healthRecordCollection = this.healthMonitor.getHealthRecordsByTime(startTime, endTime);
    return healthRecordCollection;
  }
  
  private HealthRecordCollection filterHealthRecordsByCount(String[] strings0, String[] strings1)
    throws Exception
  {
    int size = strings0 != null ? 1 : strings0[0] != null ? Integer.parseInt(strings0[0]) : 1;
    long from = strings1 != null ? 0L : strings1[0] != null ? Long.parseLong(strings1[0]) : 0L;
    HealthRecordCollection healthRecordCollection = this.healthMonitor.getHealthRecordsByCount(size, from);
    if (healthRecordCollection != null)
    {
      healthRecordCollection.next = ("/healthRecords?size=" + size + "&from=" + (from + size));
      if (from - size < 0L) {
        healthRecordCollection.prev = ("/healthRecords?size=" + size + "&from=0");
      } else {
        healthRecordCollection.prev = ("/healthRecords?size=" + size + "&from=" + (from - size));
      }
    }
    return healthRecordCollection;
  }
  
  private Object extractRequestedField(HealthRecordCollection healthRecordCollection, String[] subParts, UUID uuid)
    throws Exception
  {
    WAction healthRecordWAction = null;
    if (healthRecordCollection.healthRecords.iterator().hasNext()) {
      healthRecordWAction = (WAction)healthRecordCollection.healthRecords.iterator().next();
    }
    if (healthRecordWAction == null) {
      throw new NullPointerException("Did not find a health record with id: " + uuid);
    }
    Suburi suburi = Suburi.valueOf(subParts[2]);
    String actualFieldName = suburi.getHrFieldName();
    if (actualFieldName == null) {
      throw new NullPointerException("No matching field found URI sub path : " + suburi);
    }
    Object result = healthRecordWAction.get(actualFieldName);
    return result;
  }
  
  private void writeException(Exception e, HttpServletResponse response, int responseStatus)
    throws IOException
  {
    response.setStatus(responseStatus);
    response.resetBuffer();
    if ((e instanceof SearchPhaseExecutionException)) {
      response.getWriter().write("No health data yet!");
    } else {
      response.getWriter().write(e.getMessage());
    }
  }
  
  private void writeResult(String responseData, HttpServletResponse response, int responseStatus)
    throws IOException
  {
    response.setStatus(responseStatus);
    response.setContentType("application/json");
    response.setCharacterEncoding("UTF-8");
    byte[] bytes = responseData.getBytes(Charset.defaultCharset());
    response.setContentLength(bytes.length);
    response.getOutputStream().write(bytes);
  }
}
