package com.bloom.drop;

import com.bloom.classloading.WALoader;
import com.bloom.exception.SecurityException;
import com.bloom.exception.Warning;
import com.bloom.historicalcache.Cache;
import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.metaRepository.MDClientOps;
import com.bloom.metaRepository.MDRepository;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.metaRepository.PermissionUtility;
import com.bloom.metaRepository.RemoteCall;
import com.bloom.persistence.WactionStore;
import com.bloom.proc.BaseProcess;
import com.bloom.runtime.Context;
import com.bloom.runtime.DistributedExecutionManager;
import com.bloom.runtime.KafkaStreamUtils;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.CQExecutionPlan;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfoStatus;
import com.bloom.runtime.meta.MetaInfo.CQ;
import com.bloom.runtime.meta.MetaInfo.Dashboard;
import com.bloom.runtime.meta.MetaInfo.DeploymentGroup;
import com.bloom.runtime.meta.MetaInfo.Flow;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.Namespace;
import com.bloom.runtime.meta.MetaInfo.Page;
import com.bloom.runtime.meta.MetaInfo.PropertySet;
import com.bloom.runtime.meta.MetaInfo.PropertyVariable;
import com.bloom.runtime.meta.MetaInfo.Query;
import com.bloom.runtime.meta.MetaInfo.QueryVisualization;
import com.bloom.runtime.meta.MetaInfo.Role;
import com.bloom.runtime.meta.MetaInfo.Source;
import com.bloom.runtime.meta.MetaInfo.Stream;
import com.bloom.runtime.meta.MetaInfo.Target;
import com.bloom.runtime.meta.MetaInfo.Type;
import com.bloom.runtime.meta.MetaInfo.User;
import com.bloom.runtime.meta.MetaInfo.Visualization;
import com.bloom.runtime.meta.MetaInfo.WAStoreView;
import com.bloom.runtime.meta.MetaInfo.WActionStore;
import com.bloom.runtime.meta.MetaInfo.Window;
import com.bloom.security.ObjectPermission;
import com.bloom.security.WASecurityManager;
import com.bloom.security.ObjectPermission.Action;
import com.bloom.tungsten.Tungsten;
import com.bloom.utility.Utility;
import com.bloom.uuid.AuthToken;
import com.bloom.uuid.UUID;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.ISet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.log4j.Logger;

public class DropMetaObject
{
  public static enum DropRule
  {
    CASCADE,  FORCE,  ALL,  NONE;
    
    private DropRule() {}
  }
  
  public static EntityType[] sharedObject = { EntityType.WINDOW, EntityType.CACHE, EntityType.STREAM, EntityType.TYPE, EntityType.WACTIONSTORE };
  private static WASecurityManager securityManager = WASecurityManager.get();
  private static Logger logger = Logger.getLogger(DropMetaObject.class);
  private static MDRepository metaDataRepository = MetadataRepository.getINSTANCE();
  
  public static boolean isValidInput(MetaInfo.MetaObject metaObject, DropRule dropRule, AuthToken authToken)
  {
    if ((metaObject == null) || (dropRule == null) || (authToken == null)) {
      return false;
    }
    return true;
  }
  
  public static boolean isNamespaceEmpty(String nsName, AuthToken token)
    throws MetaDataRepositoryException
  {
    for (Iterator iterator = HazelcastSingleton.get().getSet("#" + nsName).iterator();iterator.hasNext();)
    {
    	UUID uuid = (UUID)iterator.next();
      MetaInfo.MetaObject metaObjectInNamespace = getMetaObjectByUUID(uuid, token);
      if ((!(metaObjectInNamespace instanceof MetaInfo.Role)) && (!metaObjectInNamespace.getMetaInfoStatus().isDropped())) {
        return false;
      }
    }
    return true;
  }
  
  private static String flagDropIfAnonymous(MetaInfo.MetaObject metaObject, AuthToken authToken)
    throws MetaDataRepositoryException
  {
    String str = "";
    if (metaObject == null) {
      return str;
    }
    if (metaObject.getMetaInfoStatus().isAnonymous()) {
      str = str + flagDropped(metaObject, authToken);
    }
    return str;
  }
  
  protected static String flagDropIfGenerated(MetaInfo.MetaObject metaObject, AuthToken authToken)
    throws MetaDataRepositoryException
  {
    String str = "";
    if (metaObject == null) {
      return str;
    }
    if (metaObject.getMetaInfoStatus().isGenerated()) {
      str = str + flagDropped(metaObject, authToken);
    }
    return str;
  }
  
  private static List<String> flagDropped(Context ctx, EntityType entityType, Set<UUID> metaObjectUUIDList, DropRule dropRule, AuthToken authToken)
    throws MetaDataRepositoryException
  {
    List<String> str = new ArrayList();
    for (UUID objectUUID : metaObjectUUIDList)
    {
      MetaInfo.MetaObject metaObject = getMetaObjectByUUID(objectUUID, authToken);
      try
      {
        checkPermissionToDrop(metaObject, authToken);
      }
      catch (Exception e)
      {
        str.add(e.getMessage());
      }
      if (metaObject != null)
      {
        str.addAll(dropObject(ctx, metaObject, entityType, dropRule, authToken));
        MetaInfo.MetaObject droppedObject = getMetaObjectByUUID(metaObject.getUuid(), authToken);
        if (droppedObject != null) {
          str.add(flagDropped(droppedObject, authToken));
        }
      }
      else
      {
        throw new MetaDataRepositoryException("Meta Object with ID: " + objectUUID + " is not available");
      }
    }
    return str;
  }
  
  private static boolean isPartOfApplicationOrFlowDeployed(MetaInfo.Flow applicationOrFlow, AuthToken authToken)
    throws MetaDataRepositoryException
  {
	    Map.Entry<EntityType, LinkedHashSet<UUID>> metaObjectsInApplication;

    for (Iterator i$ = applicationOrFlow.objects.entrySet().iterator(); i$.hasNext();)
    {
      metaObjectsInApplication = (Map.Entry)i$.next();
      Set<UUID> metaObjectUUIDList = (Set)metaObjectsInApplication.getValue();
      for (UUID uuid : metaObjectUUIDList)
      {
        MetaInfo.MetaObject metaObject = getMetaObjectByUUID(uuid, authToken);
        if ((metaObject != null) && 
          (isMetaObjectDeployed(metaObject, (EntityType)metaObjectsInApplication.getKey(), authToken))) {
          return false;
        }
      }
    }
    return true;
  }
  
  public static <E extends MetaInfo.MetaObject> List<E> getAllObjectsByNamespaceAndAuthOptimized(String nsName, AuthToken token, boolean ignoresecurity)
    throws MetaDataRepositoryException
  {
    List list = new ArrayList();
    UUID uuid = null;
    for (Iterator iterator= HazelcastSingleton.get().getSet("#" + nsName).iterator();iterator.hasNext();) {
      list.add(getMetaObjectByUUID(uuid, token));
      uuid = (UUID)iterator.next();
    }
    return list;
  }
  
  public static <E extends MetaInfo.Flow> Set<E> getApplicationByNamespaceAndAuth(String nsName, AuthToken token, boolean ignoresecurity)
    throws MetaDataRepositoryException
  {
    Set list = new LinkedHashSet();
    for (Iterator iterator = HazelcastSingleton.get().getSet("#" + nsName).iterator();iterator.hasNext();)
    {
    	UUID uuid = (UUID)iterator.next();
      MetaInfo.MetaObject x = getMetaObjectByUUID(uuid, token);
      if (x.getType() == EntityType.APPLICATION) {
        list.add(x);
      }
    }
    return list;
  }
  
  public static List<String> dropObject(Context ctx, MetaInfo.MetaObject objectToDrop, EntityType objectType, DropRule dropRule, AuthToken authToken)
    throws MetaDataRepositoryException
  {
    List<String> str = new ArrayList();
    if (objectToDrop.getMetaInfoStatus().isDropped()) {
      return str;
    }
    switch (objectType)
    {
    case ALERTSUBSCRIBER: 
      break;
    case APPLICATION: 
      str.addAll(DropApplication.drop(ctx, objectToDrop, dropRule, authToken));
      break;
    case CACHE: 
      str.addAll(DropCache.drop(ctx, objectToDrop, dropRule, authToken));
      break;
    case CQ: 
      str.addAll(DropCQ.drop(ctx, objectToDrop, dropRule, authToken));
      break;
    case DG: 
      break;
    case FLOW: 
      str.addAll(DropFlow.drop(ctx, objectToDrop, dropRule, authToken));
      break;
    case INITIALIZER: 
      break;
    case PROPERTYSET: 
      break;
    case PROPERTYVARIABLE: 
      break;
    case PROPERTYTEMPLATE: 
      break;
    case ROLE: 
      str.addAll(DropRole.drop(ctx, objectToDrop, dropRule, authToken));
      break;
    case SERVER: 
      break;
    case SOURCE: 
      str.addAll(DropSource.drop(ctx, objectToDrop, dropRule, authToken));
      break;
    case STREAM: 
      str.addAll(DropStream.drop(ctx, objectToDrop, dropRule, authToken));
      break;
    case STREAM_GENERATOR: 
      break;
    case TARGET: 
      str.addAll(DropTarget.drop(ctx, objectToDrop, dropRule, authToken));
      break;
    case TYPE: 
      str.addAll(DropType.drop(ctx, objectToDrop, dropRule, authToken));
      break;
    case USER: 
      str.addAll(DropUser.drop(ctx, objectToDrop, dropRule, authToken));
      break;
    case VISUALIZATION: 
      str.addAll(DropVisualization.drop(ctx, objectToDrop, dropRule, authToken));
      break;
    case WACTIONSTORE: 
      str.addAll(DropWactionStore.drop(ctx, objectToDrop, dropRule, authToken));
      break;
    case WINDOW: 
      str.addAll(DropWindow.drop(ctx, objectToDrop, dropRule, authToken));
      break;
    case UNKNOWN: 
      logger.warn("Cannot drop " + objectToDrop.getType() + " " + objectToDrop.getFullName() + ".\n");
      str.add("Cannot drop " + objectToDrop.getType() + " " + objectToDrop.getFullName() + ".\n");
      break;
    case DASHBOARD: 
      str.addAll(DropDashboard.drop(ctx, objectToDrop, dropRule, authToken));
      break;
    case WASTOREVIEW: 
      metaDataRepository.removeMetaObjectByUUID(objectToDrop.getUuid(), authToken);
      break;
    case PAGE: 
    case QUERYVISUALIZATION: 
      str.addAll(DropDefault.drop(ctx, objectToDrop, dropRule, authToken));
      break;
    default: 
      str.add("Cannot drop " + objectToDrop.getType() + " " + objectToDrop.getFullName() + ".\n");
    }
    return str;
  }
  
  private static void checkPermissionToDrop(MetaInfo.MetaObject metaObject, AuthToken authToken)
    throws MetaDataRepositoryException
  {
    PermissionUtility.checkPermission(metaObject, ObjectPermission.Action.drop, authToken, true);
  }
  
  private static String flagDropped(MetaInfo.MetaObject metaObject, AuthToken authToken)
    throws MetaDataRepositoryException
  {
    if (metaObject.getMetaInfoStatus().isDropped()) {
      return "";
    }
    metaDataRepository.removeStatusInfo(metaObject.getUuid(), WASecurityManager.TOKEN);
    metaObject.getMetaInfoStatus().setDropped(true);
    removeFromRecentVersion(metaObject.nsName, metaObject.uuid);
    metaDataRepository.updateMetaObject(metaObject, authToken);
    invalidateRelatedObjects(metaObject, authToken);
    if (metaObject.type == EntityType.TYPE)
    {
      if (((MetaInfo.Type)metaObject).generated.booleanValue()) {
        if (metaObject.getMetaInfoStatus().isGenerated())
        {
          ((MetaInfo.Type)metaObject).removeClass();
        }
        else
        {
          ((MetaInfo.Type)metaObject).removeClass();
          return metaObject.type + " " + metaObject.name + " dropped successfully\n";
        }
      }
    }
    else if (metaObject.type.equals(EntityType.WACTIONSTORE))
    {
      ((MetaInfo.WActionStore)metaObject).removeGeneratedClasses();
      return metaObject.type + " " + metaObject.name + " dropped successfully\n";
    }
    if ((metaObject.type == EntityType.TYPE) && (!((MetaInfo.Type)metaObject).generated.booleanValue())) {
      return "";
    }
    if (!metaObject.getMetaInfoStatus().isAnonymous())
    {
      if (metaObject.type == EntityType.CACHE)
      {
        MetaInfo.Cache cacheMetaObject = (MetaInfo.Cache)metaObject;
        if (cacheMetaObject.adapterClassName == null) {
          return "EVENTTABLE " + metaObject.name + " dropped successfully\n";
        }
      }
      return metaObject.type + " " + metaObject.name + " dropped successfully\n";
    }
    return "";
  }
  
  public static void removeFromRecentVersion(String namespaceName, UUID objectName)
  {
    HazelcastSingleton.get().getSet("#" + namespaceName).remove(objectName);
  }
  
  public static void addToRecentVersion(String namespaceName, UUID objectName)
  {
    HazelcastSingleton.get().getSet("#" + namespaceName).add(objectName);
  }
  
  private static void invalidateRelatedObjects(MetaInfo.MetaObject metaObject, AuthToken authToken)
    throws MetaDataRepositoryException
  {
    if (metaObject == null) {
      return;
    }
    Set<UUID> relatedObject = metaObject.getReverseIndexObjectDependencies();
    if ((relatedObject == null) || (relatedObject.isEmpty())) {
      return;
    }
    for (UUID uuid : relatedObject)
    {
      MetaInfo.MetaObject relatedMetaObject = getMetaObjectByUUID(uuid, authToken);
      if (relatedMetaObject != null)
      {
        if ((!relatedMetaObject.getMetaInfoStatus().isDropped()) && (relatedMetaObject.getMetaInfoStatus().isValid()))
        {
          relatedMetaObject.getMetaInfoStatus().setValid(false);
          metaDataRepository.updateMetaObject(relatedMetaObject, authToken);
          if (logger.isDebugEnabled()) {
            logger.debug("Related Object " + relatedMetaObject.name);
          }
        }
        invalidateRelatedObjects(relatedMetaObject, authToken);
      }
      else if (logger.isInfoEnabled())
      {
        logger.info(metaObject.getFullName() + " is already dropped.");
      }
    }
  }
  
  private static void updateCurrentAppAndFlowPointers(Context context)
    throws MetaDataRepositoryException
  {
    if (context == null) {
      return;
    }
    if (context.getCurApp() != null) {
      context.setCurApp((MetaInfo.Flow)context.get(context.getCurApp().nsName + "." + context.getCurApp().name, EntityType.APPLICATION));
    }
    if (context.getCurFlow() != null) {
      context.setCurFlow((MetaInfo.Flow)context.get(context.getCurFlow().nsName + "." + context.getCurFlow().name, EntityType.FLOW));
    }
  }
  
  private static List<MetaInfo.Flow> getAppsContainMetaObject(MetaInfo.MetaObject metaObject, AuthToken authToken)
    throws MetaDataRepositoryException
  {
    List<MetaInfo.Flow> appsContainThisMetaObject = new ArrayList();
    if (metaObject.type == EntityType.APPLICATION)
    {
      appsContainThisMetaObject.add((MetaInfo.Flow)metaObject);
    }
    else if (metaObject.type == EntityType.NAMESPACE)
    {
      List<MetaInfo.MetaObject> allObjectsInNamespace = getAllObjectsByNamespaceAndAuthOptimized(metaObject.getName(), authToken, false);
      if (allObjectsInNamespace != null) {
        for (MetaInfo.MetaObject objectInNamespace : allObjectsInNamespace) {
          if ((objectInNamespace != null) && (objectInNamespace.type == EntityType.APPLICATION)) {
            appsContainThisMetaObject.add((MetaInfo.Flow)objectInNamespace);
          }
        }
      }
    }
    else
    {
      Set<UUID> dependencyList = metaObject.getReverseIndexObjectDependencies();
      for (UUID dependency : dependencyList)
      {
        MetaInfo.MetaObject depenedentObject = getMetaObjectByUUID(dependency, authToken);
        if (depenedentObject != null)
        {
          if (depenedentObject.type == EntityType.APPLICATION) {
            appsContainThisMetaObject.add((MetaInfo.Flow)depenedentObject);
          }
        }
        else if (logger.isInfoEnabled()) {
          logger.info(metaObject.getFullName() + " is already dropped.");
        }
      }
    }
    return appsContainThisMetaObject;
  }
  
  private static boolean isMetaObjectDeployed(MetaInfo.MetaObject metaObject, EntityType entityType, AuthToken authToken)
    throws MetaDataRepositoryException
  {
    List<MetaInfo.Flow> appsContainMetaObject = new ArrayList();
    appsContainMetaObject = getAppsContainMetaObject(metaObject, authToken);
    if (logger.isDebugEnabled()) {
      logger.debug("checkIfAppDeployed() => appsContainMetaObject " + appsContainMetaObject);
    }
    for (MetaInfo.Flow app : appsContainMetaObject)
    {
      if (logger.isDebugEnabled())
      {
        logger.debug("-->" + app.name);
        logger.debug("-->" + app.deploymentPlan);
      }
      if (app.deploymentPlan != null)
      {
        if (metaObject.type == EntityType.APPLICATION) {
          throw new RuntimeException("Cannot remove, " + metaObject.name + " is deployed!! (Undeploy application first)");
        }
        throw new RuntimeException("Cannot remove, app/flow " + app.name + " that uses " + metaObject.name + " is deployed!! (Undeploy application first)");
      }
    }
    return false;
  }
  
  private static void checkAndUndeploy(MetaInfo.MetaObject metaObject, EntityType entityType, AuthToken authToken) {}
  
  private static MetaInfo.MetaObject getMetaObjectByUUID(UUID metaObjectUUID, AuthToken authToken)
    throws MetaDataRepositoryException
  {
    MetaInfo.MetaObject metaObject = metaDataRepository.getMetaObjectByUUID(metaObjectUUID, authToken);
    return metaObject;
  }
  
  public static class DropRole
  {
    public static List<String> drop(Context ctx, MetaInfo.MetaObject metaObject, DropMetaObject.DropRule dropRule, AuthToken authToken)
    {
      MetaInfo.Role roleObject = (MetaInfo.Role)metaObject;
      List<String> resultString = new ArrayList();
      try
      {
        DropMetaObject.securityManager.removeRole(roleObject, authToken);
      }
      catch (SecurityException errorMessage)
      {
        if (errorMessage != null)
        {
          resultString.add(errorMessage.getMessage());
          return resultString;
        }
      }
      catch (Exception e) {}
      return resultString;
    }
  }
  
  public static class DropUser
  {
    public static List<String> drop(Context ctx, MetaInfo.MetaObject metaObject, DropMetaObject.DropRule dropRule, AuthToken authToken)
    {
      MetaInfo.User userObject = (MetaInfo.User)metaObject;
      List<String> resultString = new ArrayList();
      try
      {
        if (userObject == null) {
          DropMetaObject.securityManager.removeUser(Tungsten.currUserMetaInfo.getName(), null, authToken);
        } else {
          DropMetaObject.securityManager.removeUser(Tungsten.currUserMetaInfo.getName(), userObject.getUserId(), authToken);
        }
      }
      catch (SecurityException errorMessage)
      {
        throw errorMessage;
      }
      catch (Exception e) {}
      return resultString;
    }
  }
  
  public static class DropCache
  {
    public static List<String> drop(Context ctx, MetaInfo.MetaObject metaObject, DropMetaObject.DropRule dropRule, AuthToken authToken)
      throws MetaDataRepositoryException
    {
      MetaInfo.Cache cacheMetaObject = (MetaInfo.Cache)metaObject;
      List<String> resultString = new ArrayList();
      if ((dropRule == DropMetaObject.DropRule.FORCE) || (dropRule == DropMetaObject.DropRule.ALL))
      {
        DropMetaObject.checkAndUndeploy(cacheMetaObject, EntityType.CACHE, authToken);
        return resultString;
      }
      if (DropMetaObject.isMetaObjectDeployed(cacheMetaObject, EntityType.CACHE, authToken)) {
        return resultString;
      }
      try
      {
        DropMetaObject.checkPermissionToDrop(cacheMetaObject, authToken);
      }
      catch (Exception e)
      {
        resultString.add(e.getMessage());
        return resultString;
      }
      resultString.add(DropMetaObject.flagDropped(cacheMetaObject, authToken));
      
      resultString.add(DropMetaObject.flagDropIfAnonymous(DropMetaObject.getMetaObjectByUUID(cacheMetaObject.typename, authToken), authToken));
      
      Cache.drop(cacheMetaObject);
      
      DropMetaObject.updateCurrentAppAndFlowPointers(ctx);
      return resultString;
    }
  }
  
  public static class DropCQ
  {
    public static List<String> drop(Context ctx, MetaInfo.MetaObject metaObject, DropMetaObject.DropRule dropRule, AuthToken authToken)
      throws MetaDataRepositoryException
    {
      MetaInfo.CQ cqMetaObject = (MetaInfo.CQ)metaObject;
      List<String> resultString = new ArrayList();
      if ((dropRule == DropMetaObject.DropRule.FORCE) || (dropRule == DropMetaObject.DropRule.ALL))
      {
        DropMetaObject.checkAndUndeploy(cqMetaObject, EntityType.CQ, authToken);
        return resultString;
      }
      if (DropMetaObject.isMetaObjectDeployed(cqMetaObject, EntityType.CQ, authToken)) {
        return resultString;
      }
      try
      {
        DropMetaObject.checkPermissionToDrop(cqMetaObject, authToken);
      }
      catch (Exception e)
      {
        resultString.add(e.getMessage());
        return resultString;
      }
      resultString.add(DropMetaObject.flagDropped(cqMetaObject, authToken));
      for (UUID ds : cqMetaObject.plan.getDataSources())
      {
        MetaInfo.MetaObject cqSubtaskMetaObject = DropMetaObject.getMetaObjectByUUID(ds, authToken);
        if ((cqSubtaskMetaObject != null) && (cqSubtaskMetaObject.getMetaInfoStatus().isAnonymous()) && (!(cqSubtaskMetaObject instanceof MetaInfo.Stream)) && (!(cqSubtaskMetaObject instanceof MetaInfo.WAStoreView))) {
          resultString.add(DropMetaObject.flagDropped(cqSubtaskMetaObject, authToken));
        }
      }
      if (cqMetaObject.stream != null)
      {
        MetaInfo.MetaObject cqOutputStreamMetaObject = DropMetaObject.getMetaObjectByUUID(cqMetaObject.stream, authToken);
        if ((cqOutputStreamMetaObject != null) && (cqOutputStreamMetaObject.getMetaInfoStatus().isAnonymous()) && ((cqOutputStreamMetaObject instanceof MetaInfo.Stream)))
        {
          if (DropMetaObject.logger.isDebugEnabled()) {
            DropMetaObject.logger.debug("AGAIN " + cqOutputStreamMetaObject.getName() + " " + cqOutputStreamMetaObject.getMetaInfoStatus());
          }
          resultString.addAll(DropMetaObject.DropStream.drop(ctx, cqOutputStreamMetaObject, dropRule, authToken));
        }
      }
      DropMetaObject.updateCurrentAppAndFlowPointers(ctx);
      return resultString;
    }
  }
  
  public static class DropSource
  {
    public static List<String> drop(Context ctx, MetaInfo.MetaObject metaObject, DropMetaObject.DropRule dropRule, AuthToken authToken)
      throws MetaDataRepositoryException
    {
      List<String> resultString = new ArrayList();
      MetaInfo.Source sourceMetaObject = (MetaInfo.Source)metaObject;
      if ((dropRule == DropMetaObject.DropRule.FORCE) || (dropRule == DropMetaObject.DropRule.ALL))
      {
        DropMetaObject.checkAndUndeploy(sourceMetaObject, EntityType.SOURCE, authToken);
        return resultString;
      }
      if (DropMetaObject.isMetaObjectDeployed(sourceMetaObject, EntityType.SOURCE, authToken)) {
        return resultString;
      }
      try
      {
        DropMetaObject.checkPermissionToDrop(sourceMetaObject, authToken);
      }
      catch (Exception e)
      {
        resultString.add(e.getMessage());
        return resultString;
      }
      boolean isDependent = checkIfStreamIsDependent(sourceMetaObject, authToken);
      if (!isDependent) {
        if (!sourceMetaObject.coDependentObjects.isEmpty())
        {
          for (UUID uuid : sourceMetaObject.coDependentObjects)
          {
            MetaInfo.MetaObject obj = DropMetaObject.getMetaObjectByUUID(uuid, authToken);
            if ((obj.getMetaInfoStatus().isGenerated()) && (obj.getType() == EntityType.STREAM))
            {
              if (DropMetaObject.logger.isDebugEnabled()) {
                DropMetaObject.logger.debug("OBJECT IS GENERATED AND STREAM" + obj.getFullName());
              }
              DropMetaObject.DropStream.drop(ctx, obj, dropRule, authToken);
            }
            else if (obj.getMetaInfoStatus().isAnonymous())
            {
              if (DropMetaObject.logger.isDebugEnabled()) {
                DropMetaObject.logger.debug(obj.getType() + " OBJECT IS ANONYMOUS " + obj.getFullName());
              }
              DropMetaObject.flagDropIfAnonymous(obj, authToken);
            }
            else if (DropMetaObject.logger.isDebugEnabled())
            {
              DropMetaObject.logger.debug("Error case for " + obj + " %% " + obj.getMetaInfoStatus() + " %% " + obj.getType());
            }
          }
          MetadataRepository.getINSTANCE().removeMetaObjectByUUID(sourceMetaObject.outputStream, WASecurityManager.TOKEN);
        }
        else
        {
          resultString.add(DropMetaObject.flagDropIfGenerated(DropMetaObject.getMetaObjectByUUID(sourceMetaObject.outputStream, authToken), authToken));
        }
      }
      try
      {
    	  BaseProcess proc;
        Class<?> adapterFactory = WALoader.get().loadClass(sourceMetaObject.adapterClassName);
        proc = (BaseProcess)adapterFactory.newInstance();
      }
      catch (Exception ex)
      {
        
        DropMetaObject.logger.error("failed to create an instance of adapter : " + sourceMetaObject.adapterClassName, ex);
      }
      resultString.add(DropMetaObject.flagDropped(sourceMetaObject, authToken));
      
      DropMetaObject.updateCurrentAppAndFlowPointers(ctx);
      return resultString;
    }
    
    private static boolean checkIfStreamIsDependent(MetaInfo.Source sourceMetaObject, AuthToken authToken)
      throws MetaDataRepositoryException
    {
      MetaInfo.MetaObject outputStream = DropMetaObject.getMetaObjectByUUID(sourceMetaObject.outputStream, authToken);
      if (outputStream != null)
      {
        Set<UUID> objectsUsingSource = outputStream.getReverseIndexObjectDependencies();
        for (UUID uuid : objectsUsingSource)
        {
          MetaInfo.MetaObject reverseObj = DropMetaObject.getMetaObjectByUUID(uuid, authToken);
          if (reverseObj.type == EntityType.SOURCE) {
            if ((reverseObj.type == EntityType.SOURCE) && (!reverseObj.equals(sourceMetaObject))) {
              return true;
            }
          }
        }
      }
      return false;
    }
  }
  
  public static class DropStream
  {
    public static List<String> drop(Context ctx, MetaInfo.MetaObject metaObject, DropMetaObject.DropRule dropRule, AuthToken authToken)
      throws MetaDataRepositoryException
    {
      List<String> resultString = new ArrayList();
      
      MetaInfo.Stream streamMetaObject = (MetaInfo.Stream)metaObject;
      if ((dropRule == DropMetaObject.DropRule.FORCE) || (dropRule == DropMetaObject.DropRule.ALL))
      {
        DropMetaObject.checkAndUndeploy(streamMetaObject, EntityType.STREAM, authToken);
        return resultString;
      }
      if (DropMetaObject.isMetaObjectDeployed(streamMetaObject, EntityType.STREAM, authToken)) {
        return resultString;
      }
      try
      {
        DropMetaObject.checkPermissionToDrop(streamMetaObject, authToken);
      }
      catch (Exception e)
      {
        resultString.add(e.getMessage());
        return resultString;
      }
      if (((MetaInfo.Stream)metaObject).pset != null) {
        try
        {
          if (HazelcastSingleton.isClientMember())
          {
            RemoteCall deleteTopic_executor = KafkaStreamUtils.getDeleteTopicExecutor((MetaInfo.Stream)metaObject);
            DistributedExecutionManager.execOnAny(HazelcastSingleton.get(), deleteTopic_executor);
          }
          else
          {
            KafkaStreamUtils.deleteTopic((MetaInfo.Stream)metaObject);
          }
        }
        catch (Exception e)
        {
          throw new RuntimeException("Failed to delete kafka topics associated with stream: " + metaObject.getFullName() + ", Reason: " + e.getMessage(), e);
        }
      }
      resultString.add(DropMetaObject.flagDropped(streamMetaObject, authToken));
      MetaInfo.MetaObject mObject = DropMetaObject.getMetaObjectByUUID(streamMetaObject.dataType, authToken);
      if (mObject != null)
      {
        DropMetaObject.flagDropIfAnonymous(mObject, authToken);
        DropMetaObject.flagDropIfGenerated(mObject, authToken);
      }
      DropMetaObject.updateCurrentAppAndFlowPointers(ctx);
      return resultString;
    }
  }
  
  public static class DropTarget
  {
    public static List<String> drop(Context ctx, MetaInfo.MetaObject metaObject, DropMetaObject.DropRule dropRule, AuthToken authToken)
      throws MetaDataRepositoryException
    {
      List<String> resultString = new ArrayList();
      
      MetaInfo.Target targetMetaObject = (MetaInfo.Target)metaObject;
      if ((dropRule == DropMetaObject.DropRule.FORCE) || (dropRule == DropMetaObject.DropRule.ALL))
      {
        DropMetaObject.checkAndUndeploy(targetMetaObject, EntityType.TARGET, authToken);
        return resultString;
      }
      if (DropMetaObject.isMetaObjectDeployed(targetMetaObject, EntityType.TARGET, authToken)) {
        return resultString;
      }
      try
      {
        DropMetaObject.checkPermissionToDrop(targetMetaObject, authToken);
      }
      catch (Exception e)
      {
        resultString.add(e.getMessage());
        return resultString;
      }
      resultString.add(DropMetaObject.flagDropped(targetMetaObject, authToken));
      
      MetaInfo.MetaObject inputStream = DropMetaObject.getMetaObjectByUUID(targetMetaObject.inputStream, authToken);
      if (!DropMetaObject.checkIfTargetHasDependencies(targetMetaObject, inputStream, authToken)) {
        if (!inputStream.getMetaInfoStatus().isGenerated()) {
          resultString.add(DropMetaObject.flagDropIfAnonymous(inputStream, authToken));
        }
      }
      DropMetaObject.updateCurrentAppAndFlowPointers(ctx);
      return resultString;
    }
  }
  
  static boolean checkIfTargetHasDependencies(MetaInfo.MetaObject inputOutputMetaObject, MetaInfo.MetaObject connectedMetaObject, AuthToken authToken)
    throws MetaDataRepositoryException
  {
    Set<UUID> reverseIndexObjectDependencies = connectedMetaObject.getReverseIndexObjectDependencies();
    boolean hasSourceDependencies = false;
    if (reverseIndexObjectDependencies == null) {
      return hasSourceDependencies;
    }
    for (UUID uuid : reverseIndexObjectDependencies)
    {
      MetaInfo.MetaObject reverseObj = getMetaObjectByUUID(uuid, authToken);
      if ((reverseObj != null) && 
      
        (!reverseObj.getMetaInfoStatus().isDropped()) && 
        
        (!reverseObj.equals(inputOutputMetaObject))) {
        hasSourceDependencies = true;
      }
    }
    return hasSourceDependencies;
  }
  
  public static class DropType
  {
    public static List<String> drop(Context ctx, MetaInfo.MetaObject metaObject, DropMetaObject.DropRule dropRule, AuthToken authToken)
      throws MetaDataRepositoryException
    {
      List<String> resultString = new ArrayList();
      MetaInfo.Type typeMetaObject = (MetaInfo.Type)metaObject;
      if ((dropRule == DropMetaObject.DropRule.FORCE) || (dropRule == DropMetaObject.DropRule.ALL))
      {
        DropMetaObject.checkAndUndeploy(typeMetaObject, EntityType.TYPE, authToken);
        return resultString;
      }
      if (DropMetaObject.isMetaObjectDeployed(typeMetaObject, EntityType.TYPE, authToken)) {
        return resultString;
      }
      try
      {
        DropMetaObject.checkPermissionToDrop(typeMetaObject, authToken);
      }
      catch (Exception e)
      {
        resultString.add(e.getMessage());
        return resultString;
      }
      resultString.add(DropMetaObject.flagDropped(typeMetaObject, authToken));
      
      DropMetaObject.updateCurrentAppAndFlowPointers(ctx);
      return resultString;
    }
  }
  
  public static class DropWactionStore
  {
    public static List<String> drop(Context ctx, MetaInfo.MetaObject metaObject, DropMetaObject.DropRule dropRule, AuthToken authToken)
      throws MetaDataRepositoryException
    {
      if (HazelcastSingleton.isClientMember()) {
        try
        {
          MDClientOps clientOps = MDClientOps.getINSTANCE();
          String wActionStoreName = metaObject.getFullName();
          List<String> result = clientOps.drop(wActionStoreName, EntityType.WACTIONSTORE, dropRule, authToken);
          DropMetaObject.updateCurrentAppAndFlowPointers(ctx);
          return result;
        }
        catch (Exception exception)
        {
          Throwable cause = exception;
          while (cause.getCause() != null) {
            cause = cause.getCause();
          }
          throw new RuntimeException(cause);
        }
      }
      MetaInfo.WActionStore wactionStoreObject = (MetaInfo.WActionStore)metaObject;
      List<String> resultString = new ArrayList();
      if ((dropRule == DropMetaObject.DropRule.FORCE) || (dropRule == DropMetaObject.DropRule.ALL))
      {
        WactionStore.drop(wactionStoreObject);
        DropMetaObject.checkAndUndeploy(wactionStoreObject, EntityType.WACTIONSTORE, authToken);
        return resultString;
      }
      if (DropMetaObject.isMetaObjectDeployed(wactionStoreObject, EntityType.WACTIONSTORE, authToken))
      {
        DropMetaObject.logger.warn("WactionStore " + wactionStoreObject.getName() + " is deployed so not dropping it");
        return resultString;
      }
      try
      {
        DropMetaObject.checkPermissionToDrop(wactionStoreObject, authToken);
      }
      catch (Exception e)
      {
        DropMetaObject.logger.warn("Failed to drop wactionstore with exception " + e.getMessage());
        resultString.add(e.getMessage());
        return resultString;
      }
      WactionStore.drop(wactionStoreObject);
      resultString.add(DropMetaObject.flagDropped(wactionStoreObject, authToken));
      
      MetaInfo.Type wactionContextType = (MetaInfo.Type)DropMetaObject.getMetaObjectByUUID(wactionStoreObject.contextType, authToken);
      if (wactionContextType.getMetaInfoStatus().isAnonymous()) {
        resultString.add(DropMetaObject.flagDropped(wactionContextType, authToken));
      }
      if (wactionStoreObject.eventTypes != null) {
        for (UUID wactionEventUUID : wactionStoreObject.eventTypes)
        {
          MetaInfo.Type wactionEventType = (MetaInfo.Type)DropMetaObject.getMetaObjectByUUID(wactionEventUUID, authToken);
          if (wactionEventType.getMetaInfoStatus().isAnonymous()) {
            resultString.add(DropMetaObject.flagDropped(wactionEventType, authToken));
          }
        }
      }
      DropMetaObject.updateCurrentAppAndFlowPointers(ctx);
      return resultString;
    }
  }
  
  public static class DropWindow
  {
    public static List<String> drop(Context ctx, MetaInfo.MetaObject metaObject, DropMetaObject.DropRule dropRule, AuthToken authToken)
      throws MetaDataRepositoryException
    {
      List<String> resultString = new ArrayList();
      MetaInfo.Window windowMetaObject = (MetaInfo.Window)metaObject;
      if ((dropRule == DropMetaObject.DropRule.FORCE) || (dropRule == DropMetaObject.DropRule.ALL))
      {
        DropMetaObject.checkAndUndeploy(windowMetaObject, EntityType.WINDOW, authToken);
        return resultString;
      }
      if (DropMetaObject.isMetaObjectDeployed(windowMetaObject, EntityType.WINDOW, authToken)) {
        return resultString;
      }
      try
      {
        DropMetaObject.checkPermissionToDrop(windowMetaObject, authToken);
      }
      catch (Exception e)
      {
        resultString.add(e.getMessage());
        return resultString;
      }
      resultString.add(DropMetaObject.flagDropped(windowMetaObject, authToken));
      
      DropMetaObject.updateCurrentAppAndFlowPointers(ctx);
      return resultString;
    }
  }
  
  public static class DropApplication
  {
    public static List<String> drop(Context ctx, MetaInfo.MetaObject metaObject, DropMetaObject.DropRule dropRule, AuthToken authToken)
      throws MetaDataRepositoryException
    {
      List<String> resultString = new ArrayList();
      if (!DropMetaObject.isValidInput(metaObject, dropRule, authToken)) {
        return resultString;
      }
      MetaInfo.Flow applicationMetaObject = (MetaInfo.Flow)metaObject;
      if ((dropRule == DropMetaObject.DropRule.FORCE) || (dropRule == DropMetaObject.DropRule.ALL))
      {
        DropMetaObject.checkAndUndeploy(applicationMetaObject, EntityType.APPLICATION, authToken);
        return resultString;
      }
      if (DropMetaObject.isMetaObjectDeployed(applicationMetaObject, EntityType.APPLICATION, authToken)) {
        return resultString;
      }
      if ((dropRule == DropMetaObject.DropRule.NONE) && (!applicationMetaObject.getAllObjects().isEmpty())) {
        throw new RuntimeException("Cannot remove, " + applicationMetaObject.name + " has objects inside, use CASCADE.");
      }
      if (dropRule == DropMetaObject.DropRule.NONE)
      {
        DropMetaObject.checkPermissionToDrop(applicationMetaObject, authToken);
        resultString.add(DropMetaObject.flagDropped(applicationMetaObject, authToken));
      }
      else if (dropRule == DropMetaObject.DropRule.CASCADE)
      {
        DropMetaObject.checkPermissionToDrop(applicationMetaObject, authToken);
        if (!DropMetaObject.isPartOfApplicationOrFlowDeployed(applicationMetaObject, authToken)) {
          return resultString;
        }
        Set<UUID> allObjects = new LinkedHashSet();
        Set<UUID> allAnonObjects = new LinkedHashSet();
        for (Map.Entry<EntityType, LinkedHashSet<UUID>> metaObjectsInApplication : applicationMetaObject.objects.entrySet())
        {
          Set<UUID> metaObjectUUIDList = (Set)metaObjectsInApplication.getValue();
          allObjects.addAll(metaObjectUUIDList);
        }
        for (UUID uuid : allObjects)
        {
          MetaInfo.MetaObject object = DropMetaObject.getMetaObjectByUUID(uuid, authToken);
          if ((object != null) && (!object.getMetaInfoStatus().isAnonymous()))
          {
            DropMetaObject.logger.warn("DROPPING" + object.getFullName());
            resultString.addAll(DropMetaObject.dropObject(ctx, object, object.getType(), dropRule, authToken));
          }
          else
          {
            allAnonObjects.add(uuid);
          }
        }
        for (UUID uuid : allAnonObjects)
        {
          MetaInfo.MetaObject object = DropMetaObject.getMetaObjectByUUID(uuid, authToken);
          if ((object != null) && (object.getMetaInfoStatus().isAnonymous())) {
            resultString.add(DropMetaObject.flagDropped(object, authToken));
          }
        }
        resultString.add(DropMetaObject.flagDropped(DropMetaObject.getMetaObjectByUUID(applicationMetaObject.uuid, authToken), authToken));
      }
      DropMetaObject.updateCurrentAppAndFlowPointers(ctx);
      return resultString;
    }
  }
  
  public static class DropFlow
  {
    public static List<String> drop(Context ctx, MetaInfo.MetaObject metaObject, DropMetaObject.DropRule dropRule, AuthToken authToken)
      throws MetaDataRepositoryException
    {
      List<String> resultString = new ArrayList();
      if (!DropMetaObject.isValidInput(metaObject, dropRule, authToken)) {
        return resultString;
      }
      MetaInfo.Flow flowMetaObject = (MetaInfo.Flow)metaObject;
      if ((dropRule == DropMetaObject.DropRule.FORCE) || (dropRule == DropMetaObject.DropRule.ALL))
      {
        DropMetaObject.checkAndUndeploy(flowMetaObject, EntityType.FLOW, authToken);
        return resultString;
      }
      if (DropMetaObject.isMetaObjectDeployed(flowMetaObject, EntityType.FLOW, authToken)) {
        return resultString;
      }
      if (dropRule == DropMetaObject.DropRule.NONE)
      {
        try
        {
          DropMetaObject.checkPermissionToDrop(flowMetaObject, authToken);
        }
        catch (Exception e)
        {
          resultString.add(e.getMessage());
          return resultString;
        }
        resultString.add(DropMetaObject.flagDropped(flowMetaObject, authToken));
      }
      else if (dropRule == DropMetaObject.DropRule.CASCADE)
      {
        if (!DropMetaObject.isPartOfApplicationOrFlowDeployed(flowMetaObject, authToken)) {
          return resultString;
        }
        for (Map.Entry<EntityType, LinkedHashSet<UUID>> metaObjectsInFlow : flowMetaObject.objects.entrySet())
        {
          Set<UUID> metaObjectUUIDList = (Set)metaObjectsInFlow.getValue();
          resultString.addAll(DropMetaObject.flagDropped(ctx, (EntityType)metaObjectsInFlow.getKey(), metaObjectUUIDList, dropRule, authToken));
        }
        try
        {
          DropMetaObject.checkPermissionToDrop(flowMetaObject, authToken);
        }
        catch (Exception e)
        {
          resultString.add(e.getMessage());
          return resultString;
        }
        resultString.add(DropMetaObject.flagDropped(flowMetaObject, authToken));
      }
      DropMetaObject.updateCurrentAppAndFlowPointers(ctx);
      return resultString;
    }
  }
  
  public static class DropNamespace
  {
    public static List<String> drop(Context ctx, MetaInfo.MetaObject metaObject, DropMetaObject.DropRule dropRule, AuthToken authToken)
      throws MetaDataRepositoryException
    {
      List<String> resultString = new ArrayList();
      if (!DropMetaObject.isValidInput(metaObject, dropRule, authToken)) {
        return resultString;
      }
      MetaInfo.Namespace namespaceMetaObject = (MetaInfo.Namespace)metaObject;
      if ((dropRule == DropMetaObject.DropRule.FORCE) || (dropRule == DropMetaObject.DropRule.ALL))
      {
        DropMetaObject.checkAndUndeploy(namespaceMetaObject, EntityType.NAMESPACE, authToken);
        return resultString;
      }
      if (DropMetaObject.isMetaObjectDeployed(namespaceMetaObject, EntityType.NAMESPACE, authToken)) {
        return resultString;
      }
      if (DropMetaObject.isNamespaceEmpty(namespaceMetaObject.name, authToken))
      {
        try
        {
          DropMetaObject.checkPermissionToDrop(namespaceMetaObject, authToken);
        }
        catch (Exception e)
        {
          resultString.add(e.getMessage());
          return resultString;
        }
        resultString.add(DropMetaObject.flagDropped(namespaceMetaObject, authToken));
        
        List<MetaInfo.MetaObject> allObjectsInNamespace = DropMetaObject.getAllObjectsByNamespaceAndAuthOptimized(namespaceMetaObject.name, authToken, false);
        if (allObjectsInNamespace != null) {
          for (MetaInfo.MetaObject metaObjectInNamespace : allObjectsInNamespace) {
            if ((metaObjectInNamespace != null) && 
            
              (metaObjectInNamespace.type == EntityType.ROLE)) {
              DropMetaObject.DropRole.drop(ctx, metaObjectInNamespace, dropRule, authToken);
            }
          }
        }
      }
      else
      {
        if (dropRule == DropMetaObject.DropRule.NONE) {
          throw new Warning("Cannot drop namespace without cascade!");
        }
        Set<MetaInfo.Flow> applicationList = DropMetaObject.getApplicationByNamespaceAndAuth(namespaceMetaObject.name, authToken, false);
        if (applicationList != null) {
          for (MetaInfo.Flow flow : applicationList) {
            if (!flow.getMetaInfoStatus().isAnonymous()) {
              resultString.addAll(DropMetaObject.DropApplication.drop(ctx, flow, dropRule, authToken));
            }
          }
        }
        List<MetaInfo.MetaObject> allObjectsInNamespace = DropMetaObject.getAllObjectsByNamespaceAndAuthOptimized(metaObject.getName(), authToken, false);
        if ((dropRule != DropMetaObject.DropRule.CASCADE) && (dropRule != DropMetaObject.DropRule.ALL))
        {
          resultString.add("There are objects in namespace " + metaObject.name + ". Use CASCADE\n");
        }
        else
        {
          dropAllQueries(ctx, resultString, allObjectsInNamespace, authToken);
          
          allObjectsInNamespace = DropMetaObject.getAllObjectsByNamespaceAndAuthOptimized(metaObject.getName(), authToken, false);
          if (allObjectsInNamespace != null) {
            for (MetaInfo.MetaObject metaObjectInNamespace : allObjectsInNamespace) {
              if ((metaObjectInNamespace != null) && 
              
                (metaObjectInNamespace.type != EntityType.ROLE))
              {
                try
                {
                  DropMetaObject.checkPermissionToDrop(metaObjectInNamespace, authToken);
                }
                catch (Exception e)
                {
                  resultString.add(e.getMessage());
                }
                
                resultString.addAll(DropMetaObject.dropObject(ctx, metaObjectInNamespace, metaObjectInNamespace.type, dropRule, authToken));
              }
            }
          }
          try
          {
            DropMetaObject.checkPermissionToDrop(namespaceMetaObject, authToken);
          }
          catch (Exception e)
          {
            resultString.add(e.getMessage());
            return resultString;
          }
          resultString.add(DropMetaObject.flagDropped(namespaceMetaObject, authToken));
        }
        if (allObjectsInNamespace != null) {
          for (MetaInfo.MetaObject metaObjectInNamespace : allObjectsInNamespace) {
            if ((metaObjectInNamespace != null) && 
            
              (metaObjectInNamespace.type == EntityType.ROLE)) {
              DropMetaObject.DropRole.drop(ctx, metaObjectInNamespace, dropRule, authToken);
            }
          }
        }
      }
      DropMetaObject.updateCurrentAppAndFlowPointers(ctx);
      HazelcastSingleton.get().getList("#" + namespaceMetaObject.name).destroy();
      return resultString;
    }
    
    private static void dropAllQueries(Context context, List<String> resultString, List<MetaInfo.MetaObject> allObjectsInNamespace, AuthToken authToken)
      throws MetaDataRepositoryException
    {
      if (allObjectsInNamespace == null) {
        return;
      }
      for (MetaInfo.MetaObject metaObject : allObjectsInNamespace) {
        if ((metaObject != null) && (metaObject.getType() == EntityType.QUERY))
        {
          MetaInfo.Query queryMetaObject = (MetaInfo.Query)metaObject;
          if (queryMetaObject.appUUID != null)
          {
            MetaInfo.MetaObject obj;
            if ((obj = DropMetaObject.getMetaObjectByUUID(queryMetaObject.appUUID, authToken)) != null)
            {
              if (DropMetaObject.isMetaObjectDeployed(obj, EntityType.APPLICATION, authToken)) {
                throw new RuntimeException("Cannot remove, " + queryMetaObject.name + " is deployed!! (Stop query first)");
              }
            }
            else if (DropMetaObject.logger.isInfoEnabled()) {
              DropMetaObject.logger.info(metaObject + " has null uuid for application");
            }
          }
        }
      }
      for (MetaInfo.MetaObject metaObject : allObjectsInNamespace) {
        if ((metaObject != null) && (metaObject.getType() == EntityType.QUERY))
        {
          context.deleteQuery((MetaInfo.Query)metaObject, authToken);
          resultString.add(metaObject.type + " " + metaObject.name + " dropped successfully\n");
        }
      }
    }
  }
  
  public static class DropVisualization
  {
    public static List<String> drop(Context ctx, MetaInfo.MetaObject metaObject, DropMetaObject.DropRule dropRule, AuthToken authToken)
      throws MetaDataRepositoryException
    {
      MetaInfo.Visualization visualizationMetaObject = (MetaInfo.Visualization)metaObject;
      List<String> resultString = new ArrayList();
      try
      {
        DropMetaObject.checkPermissionToDrop(visualizationMetaObject, authToken);
      }
      catch (Exception e)
      {
        resultString.add(e.getMessage());
        return resultString;
      }
      resultString.add(DropMetaObject.flagDropped(visualizationMetaObject, authToken));
      return resultString;
    }
  }
  
  public static class DropDG
  {
    public static List<String> drop(Context ctx, MetaInfo.MetaObject metaObject, DropMetaObject.DropRule dropRule, AuthToken authToken)
      throws MetaDataRepositoryException
    {
      List<String> resultString = new ArrayList();
      if ((dropRule == DropMetaObject.DropRule.CASCADE) || (dropRule == DropMetaObject.DropRule.FORCE)) {
        throw new Warning("Unsupported option '" + dropRule.toString() + "' for drop dg.");
      }
      if ((metaObject.type == EntityType.DG) && (EntityType.DG.isNotVersionable()))
      {
        MetaInfo.DeploymentGroup dd = (MetaInfo.DeploymentGroup)metaObject;
        DropMetaObject.metaDataRepository.removeMetaObjectByUUID(metaObject.uuid, authToken);
        DropMetaObject.metaDataRepository.removeDGInfoFromServer(metaObject, authToken);
        resultString.add(metaObject.type + " " + metaObject.name + " dropped successfully\n");
      }
      return resultString;
    }
  }
  
  public static class DropSubscription
  {
    public static List<String> drop(Context ctx, MetaInfo.MetaObject metaObject, DropMetaObject.DropRule dropRule, AuthToken authToken)
      throws MetaDataRepositoryException
    {
      MetaInfo.Target subscriptionMetaObject = (MetaInfo.Target)metaObject;
      if (!subscriptionMetaObject.isSubscription()) {
        return null;
      }
      List<String> resultString = new ArrayList();
      try
      {
        DropMetaObject.checkPermissionToDrop(subscriptionMetaObject, authToken);
      }
      catch (Exception e)
      {
        resultString.add(e.getMessage());
        return resultString;
      }
      resultString.add(DropMetaObject.flagDropped(subscriptionMetaObject, authToken));
      return resultString;
    }
  }
  
  public static class DropDashboard
  {
    public static List<String> drop(Context ctx, MetaInfo.MetaObject metaObject, DropMetaObject.DropRule dropRule, AuthToken authToken)
      throws MetaDataRepositoryException
    {
      MetaInfo.Dashboard dashboardMetaObject = (MetaInfo.Dashboard)metaObject;
      List<String> pages = dashboardMetaObject.getPages();
      List<String> resultString = new ArrayList();
      
      DropMetaObject.checkPermissionToDrop(dashboardMetaObject, authToken);
      if (pages != null)
      {
        for (String page : pages)
        {
          MetaInfo.Page pageMetaObject = null;
          if (page.indexOf(".") == -1) {
            pageMetaObject = (MetaInfo.Page)DropMetaObject.metaDataRepository.getMetaObjectByName(EntityType.PAGE, metaObject.getNsName(), Utility.splitName(page), null, authToken);
          } else {
            pageMetaObject = (MetaInfo.Page)DropMetaObject.metaDataRepository.getMetaObjectByName(EntityType.PAGE, Utility.splitDomain(page), Utility.splitName(page), null, authToken);
          }
          if (pageMetaObject != null)
          {
            try
            {
              DropMetaObject.checkPermissionToDrop(pageMetaObject, authToken);
            }
            catch (Exception e)
            {
              resultString.add(e.getMessage());
              return resultString;
            }
            List<String> qvs = pageMetaObject.getQueryVisualizations();
            if (qvs != null) {
              for (String qv : qvs)
              {
                MetaInfo.QueryVisualization queryVisualizationMetaObject = null;
                if (qv.indexOf(".") == -1) {
                  queryVisualizationMetaObject = (MetaInfo.QueryVisualization)DropMetaObject.metaDataRepository.getMetaObjectByName(EntityType.QUERYVISUALIZATION, metaObject.getNsName(), Utility.splitName(qv), null, authToken);
                } else {
                  queryVisualizationMetaObject = (MetaInfo.QueryVisualization)DropMetaObject.metaDataRepository.getMetaObjectByName(EntityType.QUERYVISUALIZATION, Utility.splitDomain(qv), Utility.splitName(qv), null, authToken);
                }
                if (queryVisualizationMetaObject != null)
                {
                  try
                  {
                    DropMetaObject.checkPermissionToDrop(queryVisualizationMetaObject, authToken);
                  }
                  catch (Exception e)
                  {
                    resultString.add(e.getMessage());
                    return resultString;
                  }
                  DropMetaObject.metaDataRepository.removeMetaObjectByUUID(queryVisualizationMetaObject.getUuid(), authToken);
                  resultString.add(queryVisualizationMetaObject.type + " " + queryVisualizationMetaObject.getFullName() + " dropped successfully\n");
                }
              }
            }
            if (pageMetaObject != null)
            {
              DropMetaObject.metaDataRepository.removeMetaObjectByUUID(pageMetaObject.getUuid(), authToken);
              resultString.add(pageMetaObject.type + " " + pageMetaObject.getFullName() + " dropped successfully\n");
            }
          }
        }
        if (dashboardMetaObject != null)
        {
          DropMetaObject.metaDataRepository.removeMetaObjectByUUID(dashboardMetaObject.getUuid(), authToken);
          resultString.add(dashboardMetaObject.type + " " + dashboardMetaObject.name + " dropped successfully\n");
        }
      }
      return resultString;
    }
  }
  
  public static class DropPropertySet
  {
    public static List<String> drop(Context ctx, MetaInfo.MetaObject metaObject, DropMetaObject.DropRule dropRule, AuthToken authToken)
      throws MetaDataRepositoryException
    {
      MetaInfo.PropertySet propertySetMetaObject = (MetaInfo.PropertySet)metaObject;
      List<String> resultString = new ArrayList();
      if ((propertySetMetaObject.getReverseIndexObjectDependencies() != null) && (!propertySetMetaObject.getReverseIndexObjectDependencies().isEmpty())) {
        for (UUID userUUID : propertySetMetaObject.getReverseIndexObjectDependencies())
        {
          MetaInfo.MetaObject dependentObject = DropMetaObject.getMetaObjectByUUID(userUUID, authToken);
          if ((dependentObject != null) && (dependentObject.getType() == EntityType.USER) && ((dependentObject instanceof MetaInfo.User)))
          {
            MetaInfo.User userMetaObject = (MetaInfo.User)dependentObject;
            if (userMetaObject != null) {
              throw new RuntimeException("User is using propertyset " + metaObject.getFullName() + " cannot drop.");
            }
          }
        }
      }
      DropMetaObject.metaDataRepository.removeMetaObjectByUUID(metaObject.getUuid(), authToken);
      resultString.add(propertySetMetaObject.getType() + " " + propertySetMetaObject.getFullName() + " dropped successfully\n");
      
      return resultString;
    }
  }
  
  public static class DropPropertyVariable
  {
    public static List<String> drop(Context ctx, MetaInfo.MetaObject metaObject, DropMetaObject.DropRule dropRule, AuthToken authToken)
      throws MetaDataRepositoryException
    {
      MetaInfo.PropertyVariable propertyVariableMetaObject = (MetaInfo.PropertyVariable)metaObject;
      List<String> resultString = new ArrayList();
      
      DropMetaObject.metaDataRepository.removeMetaObjectByUUID(metaObject.getUuid(), authToken);
      resultString.add(propertyVariableMetaObject.getType() + " " + propertyVariableMetaObject.getFullName() + " dropped successfully\n");
      
      return resultString;
    }
  }
  
  public static class DropDefault
  {
    public static List<String> drop(Context ctx, MetaInfo.MetaObject metaObject, DropMetaObject.DropRule dropRule, AuthToken authToken)
      throws MetaDataRepositoryException
    {
      List<String> resultString = new ArrayList();
      if (DropMetaObject.metaDataRepository.getMetaObjectByUUID(metaObject.getUuid(), authToken) != null)
      {
        DropMetaObject.metaDataRepository.removeMetaObjectByUUID(metaObject.getUuid(), authToken);
        resultString.add(metaObject.getType() + " " + metaObject.getFullName() + " dropped successfully\n");
      }
      return resultString;
    }
  }
}
