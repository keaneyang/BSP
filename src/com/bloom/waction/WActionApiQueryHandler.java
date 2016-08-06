package com.bloom.waction;

import com.bloom.exception.ServerException;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.metaRepository.RemoteCall;
import com.bloom.persistence.WactionStore;
import com.bloom.runtime.QueryValidator;
import com.bloom.runtime.Server;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Type;
import com.bloom.runtime.meta.MetaInfo.WActionStore;
import com.bloom.security.ObjectPermission;
import com.bloom.security.WASecurityManager;
import com.bloom.security.ObjectPermission.Action;
import com.bloom.security.ObjectPermission.PermissionType;
import com.bloom.utility.Utility;
import com.bloom.uuid.AuthToken;
import com.bloom.runtime.containers.IBatch;
import com.bloom.runtime.containers.ITaskEvent;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;

public class WActionApiQueryHandler
  implements RemoteCall<Object>
{
  private static final long serialVersionUID = -369249067395520309L;
  private static Logger logger = Logger.getLogger(WActionApiQueryHandler.class);
  private final String storeName;
  private final WactionKey key;
  private final String[] fields;
  private final Map<String, Object> filter;
  private final String queryString;
  private final Map<String, Object> queryParams;
  AuthToken token;
  
  public WActionApiQueryHandler(String storename, String queryString, Map<String, Object> queryParams, AuthToken token)
  {
    this.storeName = storename;
    this.key = null;
    this.fields = null;
    this.filter = null;
    this.token = token;
    this.queryParams = queryParams;
    this.queryString = queryString;
  }
  
  public WActionApiQueryHandler(String storeName, WactionKey key, String[] fields, Map<String, Object> filter, AuthToken token)
  {
    this.storeName = storeName;
    this.key = key;
    this.fields = ((String[])fields.clone());
    this.filter = filter;
    this.token = token;
    this.queryParams = null;
    this.queryString = null;
  }
  
  public Object call()
    throws Exception
  {
    Object mapMapMap = null;
    if (this.queryString != null)
    {
      QueryRunnerForWactionApi runner = new QueryRunnerForWactionApi();
      QueryValidator qVal = new QueryValidator(Server.server);
      runner.setQueryString(this.queryString, this.queryParams, qVal, this.token);
      ITaskEvent tE = runner.call();
      
      runner.cleanUp();
      if (logger.isDebugEnabled())
      {
        logger.debug("Size of result set as seem at WAPI using Adhoc infrastructure:" + tE.batch().size());
        logger.debug("Result Set as seen at WAPI using Adhoc infrastructure:" + tE);
      }
      WactionStore ws = WactionStore.get(this.storeName);
      
      String keyField = (String)ws.contextBeanDef.keyFields.get(0);
      Map<String, String> allFields = ws.contextBeanDef.fields;
      if ((tE != null) && (keyField != null)) {
        mapMapMap = WactionApi.covertTaskEventsToMap(tE, keyField, this.fields, ws.contextBeanDef.fields, true);
      }
      if (logger.isDebugEnabled())
      {
        logger.debug("Size of result set as seem at WAPI using old Query API:" + tE.batch().size());
        logger.debug("Result of getWactions from WAction Store using old Query API: " + tE);
      }
      return mapMapMap;
    }
    if (canFetchWactions(this.storeName, this.token))
    {
      WactionStore ws = WactionStore.get(this.storeName);
      if (ws.usesNewWActionStore())
      {
        QueryRunnerForWactionApi runner = new QueryRunnerForWactionApi();
        QueryValidator qVal = new QueryValidator(Server.server);
        
        String keyField = (String)ws.contextBeanDef.keyFields.get(0);
        Map<String, String> allFields = ws.contextBeanDef.fields;
        if ((this.fields == null) || (this.fields[0].equalsIgnoreCase("default")))
        {
          String[] implicitlyAddedFields = addImplicitFields(allFields);
          runner.setAllFields(this.storeName, implicitlyAddedFields, this.filter, this.token, keyField, false, qVal);
        }
        else if (this.fields[0].equalsIgnoreCase("default-allEvents"))
        {
          String[] implicitlyAddedFields = addImplicitFields(allFields);
          runner.setAllFields(this.storeName, implicitlyAddedFields, this.filter, this.token, keyField, true, qVal);
        }
        else if (this.fields[0].equalsIgnoreCase("eventList"))
        {
          runner.setAllFields(this.storeName, null, this.filter, this.token, null, true, qVal);
        }
        else
        {
          runner.setAllFields(this.storeName, this.fields, this.filter, this.token, keyField, false, qVal);
        }
        ITaskEvent tE = runner.call();
        
        runner.cleanUp();
        if (logger.isDebugEnabled())
        {
          logger.debug("Size of result set as seem at WAPI using Adhoc infrastructure:" + tE.batch().size());
          logger.debug("Result Set as seen at WAPI using Adhoc infrastructure:" + tE);
        }
        if ((tE != null) && (keyField != null)) {
          mapMapMap = WactionApi.covertTaskEventsToMap(tE, keyField, this.fields, ws.contextBeanDef.fields, false);
        }
        if (logger.isDebugEnabled())
        {
          logger.debug("Size of result set as seem at WAPI using old Query API:" + tE.batch().size());
          logger.debug("Result of getWactions from WAction Store using old Query API: " + tE);
        }
        return mapMapMap;
      }
      mapMapMap = ws.getWactions(this.key, this.fields, this.filter);
      
      return mapMapMap;
    }
    return null;
  }
  
  public String[] addImplicitFields(Map<String, String> allFields)
  {
    List<String> implicitlyAddedFields = new ArrayList();
    Iterator<String> fieldSet = allFields.keySet().iterator();
    while (fieldSet.hasNext()) {
      implicitlyAddedFields.add(fieldSet.next());
    }
    String[] converted = new String[implicitlyAddedFields.size()];
    return (String[])implicitlyAddedFields.toArray(converted);
  }
  
  public static boolean canFetchWactions(String storename, AuthToken token)
    throws NullPointerException, MetaDataRepositoryException
  {
    boolean isDeployed = false;
    String[] splitNames = storename.split("\\.");
    String nsName = splitNames.length == 2 ? splitNames[0] : null;
    String objName = splitNames.length == 2 ? splitNames[1] : splitNames[0];
    
    MetaInfo.WActionStore wactionStoreInfo = (MetaInfo.WActionStore)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.WACTIONSTORE, nsName, objName, null, token);
    if (wactionStoreInfo == null) {
      throw new NullPointerException("No WAction Store: " + storename + " found");
    }
    ObjectPermission permission = new ObjectPermission(wactionStoreInfo.getNsName(), ObjectPermission.Action.read, Utility.getPermissionObjectType(wactionStoreInfo.getType()), wactionStoreInfo.getName(), ObjectPermission.PermissionType.allow);
    
    boolean isAllowed = WASecurityManager.get().isAllowed(token, permission);
    if (isAllowed) {
      try
      {
        if (Server.server != null) {
          Server.server.checkNotDeployed(wactionStoreInfo);
        }
      }
      catch (ServerException e)
      {
        isDeployed = true;
      }
    }
    return isDeployed;
  }
}
