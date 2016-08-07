package com.bloom.runtime.monitor;

import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.metaRepository.RemoteCall;
import com.bloom.runtime.DistributedExecutionManager;
import com.bloom.uuid.AuthToken;
import com.bloom.uuid.UUID;
import com.bloom.waction.WactionKey;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;

public class MonitoringApiQueryHandler
  implements RemoteCall<Object>
{
  private static final long serialVersionUID = -369249067395520309L;
  private static Logger logger = Logger.getLogger(MonitoringApiQueryHandler.class);
  private final String storeName;
  private final WactionKey key;
  private final String[] fields;
  private final Map<String, Object> filter;
  private final boolean getAllWaction;
  AuthToken token;
  
  public MonitoringApiQueryHandler(String storeName, WactionKey key, String[] fields, Map<String, Object> filter, AuthToken token, boolean getAllWaction)
  {
    this.storeName = storeName;
    this.key = key;
    this.fields = ((String[])fields.clone());
    this.filter = filter;
    this.token = token;
    this.getAllWaction = getAllWaction;
  }
  
  public Object call()
    throws Exception
  {
    if (this.getAllWaction) {
      return getAllWactions();
    }
    return getWaction();
  }
  
  public Object getAllWactions()
    throws Exception
  {
    MonitorModel monitorModel = MonitorModel.getInstance();
    if (monitorModel != null) {
      return monitorModel.getAllMonitorWactions(this.fields, this.filter, this.token);
    }
    return null;
  }
  
  public Object getWaction()
    throws Exception
  {
    if ((this.key.key != null) && (!(this.key.key instanceof UUID))) {
      this.key.key = new UUID(this.key.key.toString());
    }
    this.filter.put("cacheOnly", "true");
    MonitorModel monitorModel = MonitorModel.getInstance();
    if (monitorModel != null) {
      return MonitorModel.getInstance().getMonitorWaction(this.key, this.fields, this.filter);
    }
    return null;
  }
  
  public Object getMonitorDataRemote()
    throws MonitoringServerException
  {
    Object results = null;
    UUID MonitorModelServerId = (UUID)HazelcastSingleton.get().getMap("MonitorModelToServerMap").get("MonitorModel");
    if (MonitorModelServerId == null) {
      throw new MonitoringServerException("Monitoring server is not running");
    }
    Object tR = execute(this, MonitorModelServerId);
    if ((tR instanceof Collection))
    {
      Collection coll = (Collection)tR;
      if (coll.size() != 1) {
        throw new MonitoringServerException("Expected exactly 1 map(Map<WactionKey, Map<String, Object>>) with All WActions. But got " + coll.size());
      }
      results = coll.iterator().next();
    }
    else
    {
      results = new HashMap();
    }
    return results;
  }
  
  private Object execute(MonitoringApiQueryHandler action, UUID serverId)
    throws MonitoringServerException
  {
    HazelcastInstance hz = HazelcastSingleton.get();
    
    Set<Member> srvs = DistributedExecutionManager.getAllServers(hz);
    Map<UUID, Member> valid = new HashMap();
    for (Member member : srvs) {
      valid.put(new UUID(member.getUuid()), member);
    }
    Set<Member> executeSrvs = new HashSet();
    
    Member member = (Member)valid.get(serverId);
    if (member != null) {
      executeSrvs.add(member);
    } else {
      throw new MonitoringServerException("Server " + serverId + " not found");
    }
    return DistributedExecutionManager.exec(hz, action, executeSrvs);
  }
  
  public Object get()
    throws Exception
  {
    Object result = null;
    if (this.getAllWaction) {
      result = getAllWactions();
    } else {
      result = getWaction();
    }
    if (result == null) {
      result = getMonitorDataRemote();
    }
    return result;
  }
}
