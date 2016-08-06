package com.bloom.metaRepository;

import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.utils.NamePolicy;

public class MDConstants
{
  public static final String status = "status";
  public static final String serversForDeployment = "serversForDeployment";
  public static final String ENCRYPTED_KEYWORDS_PROPERTYSET_NAME = "encryptedWords";
  public static final String ENCRYPTED_FLAG = "_encrypted";
  public static final String SUBSCRIPTION_PROP = "isSubscription";
  
  public static enum typeOfPut
  {
    STATUS_INFO,  DEPLOYMENT_INFO,  SHOWSTREAMOBJECT,  METAOBJECT,  SERVERMETAOBJECT,  POSITIONINFO;
    
    private typeOfPut() {}
  }
  
  public static enum typeOfGet
  {
    BY_UUID,  BY_NAME,  BY_ENTITY_TYPE,  BY_NAMESPACE,  BY_NAMESPACE_AND_ENTITY_TYPE,  BY_STATUSINFO,  BY_POSITIONINFO,  BY_SERVERFORDEPLOYMENT,  BY_ALL;
    
    private typeOfGet() {}
  }
  
  public static enum typeOfRemove
  {
    BY_UUID,  BY_NAME,  STATUS_INFO,  DEPLOYMENT_INFO;
    
    private typeOfRemove() {}
  }
  
  public static enum mdcListener
  {
    server,  status,  showStream,  actions,  urlToMeta,  deploymentInfo;
    
    private mdcListener() {}
  }
  
  public static void checkNullParams(String exceptionMessage, Object... args)
    throws MetaDataRepositoryException
  {
    for (Object arg : args) {
      if (arg == null) {
        throw new MetaDataRepositoryException(exceptionMessage);
      }
    }
  }
  
  public static String makeUrlWithoutVersion(String namespace, EntityType eType, String name)
  {
    return NamePolicy.makeKey(namespace + ":" + eType + ":" + name);
  }
  
  public static String makeURLWithoutVersion(MetaInfo.MetaObject mObject)
  {
    return NamePolicy.makeKey(mObject.nsName + ":" + mObject.type + ":" + mObject.name);
  }
  
  public static String makeURLWithVersion(String nvUrl, Integer version)
  {
    return NamePolicy.makeKey(nvUrl + ":" + version.intValue());
  }
  
  public static String makeURLWithVersion(EntityType eType, String namespace, String name, Integer version)
  {
    return NamePolicy.makeKey(namespace + ":" + eType + ":" + name + ":" + version.intValue());
  }
  
  public static String stripVersion(String vUrl)
  {
    return vUrl.substring(0, vUrl.lastIndexOf(":"));
  }
  
  public static class MapNames
  {
    public static final String PositionInfo = "#PositionInfo";
    public static final String ObjectVersions = "#objectVersions";
    public static final String UuidToUrl = "#uuidToUrl";
    public static final String UrlToMetaObject = "#urlToMetaObject";
    public static final String ETypeToMetaObject = "#eTypeToMetaObject";
    public static final String Status = "#status";
    public static final String ShowStream = "#showStream";
    public static final String DeploymentInfo = "deployedObjects";
    public static final String Servers = "#servers";
  }
}
