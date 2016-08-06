package com.bloom.metaRepository;

import com.bloom.exception.SecurityException;
import com.bloom.runtime.Interval;
import com.bloom.runtime.Pair;
import com.bloom.runtime.Server;
import com.bloom.runtime.compiler.Grammar;
import com.bloom.runtime.compiler.Lexer;
import com.bloom.runtime.compiler.stmts.AdapterDescription;
import com.bloom.runtime.compiler.stmts.CreateSourceOrTargetStmt;
import com.bloom.runtime.compiler.stmts.Stmt;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.CQExecutionPlan;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfoStatus;
import com.bloom.runtime.meta.MetaInfo.CQ;
import com.bloom.runtime.meta.MetaInfo.Cache;
import com.bloom.runtime.meta.MetaInfo.Flow;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.Namespace;
import com.bloom.runtime.meta.MetaInfo.Role;
import com.bloom.runtime.meta.MetaInfo.ShowStream;
import com.bloom.runtime.meta.MetaInfo.Source;
import com.bloom.runtime.meta.MetaInfo.StatusInfo;
import com.bloom.runtime.meta.MetaInfo.Stream;
import com.bloom.runtime.meta.MetaInfo.Target;
import com.bloom.runtime.meta.MetaInfo.WActionStore;
import com.bloom.runtime.meta.MetaInfo.Window;
import com.bloom.runtime.meta.MetaInfo.StatusInfo.Status;
import com.bloom.security.ObjectPermission;
import com.bloom.security.WASecurityManager;
import com.bloom.security.ObjectPermission.Action;
import com.bloom.utility.Utility;
import com.bloom.uuid.AuthToken;
import com.bloom.uuid.UUID;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ILock;
import com.hazelcast.core.ISet;
import com.hazelcast.core.MessageListener;
import com.bloom.recovery.Position;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class MetadataRepository
  implements MDRepository
{
  private final MDCache mdc;
  private final MetaDataDBOps mdDBOps;
  private final VersionManager versionManager;
  private WASecurityManager securityManager;
  private final boolean doPersist;
  private static MetadataRepository INSTANCE = new MetadataRepository();
  private static Logger logger = Logger.getLogger(MetadataRepository.class);
  
  private MetadataRepository()
  {
    this.mdc = MDCache.getInstance();
    this.versionManager = VersionManager.getInstance();
    String persist = System.getProperty("com.bloom.config.persist");
    if (persist != null)
    {
      if ((!HazelcastSingleton.isClientMember()) && (persist.equalsIgnoreCase("true")))
      {
        this.doPersist = true;
        this.mdDBOps = MetaDataDBOps.getInstance();
      }
      else
      {
        this.doPersist = false;
        this.mdDBOps = null;
      }
    }
    else
    {
      this.doPersist = false;
      this.mdDBOps = null;
    }
  }
  
  public void initialize()
    throws MetaDataRepositoryException
  {
    this.securityManager = WASecurityManager.get();
    if (this.doPersist)
    {
      if (HazelcastSingleton.get().getAtomicLong("#MDInitialized").get() == 1L) {
        return;
      }
      ILock lock = HazelcastSingleton.get().getLock("MetaRepoLock");
      lock.lock();
      try
      {
        if (HazelcastSingleton.get().getAtomicLong("#MDInitialized").get() == 1L) {
          return;
        }
        loadCacheFromDb();
        HazelcastSingleton.get().getAtomicLong("#MDInitialized").set(1L);
      }
      finally
      {
        lock.unlock();
      }
    }
  }
  
  private void loadCacheFromDb()
    throws MetaDataRepositoryException
  {
    List<MetaInfo.MetaObject> allObjects = (List)this.mdDBOps.get(null, null, null, null, null, MDConstants.typeOfGet.BY_ALL);
    for (MetaInfo.MetaObject metaObject : allObjects)
    {
      if (logger.isInfoEnabled()) {
        logger.info("Putting MetaObject with URI: " + metaObject.getUri() + " from Database");
      }
      if (this.versionManager.put(metaObject))
      {
        this.mdc.put(metaObject);
        
        MetaInfo.StatusInfo status = new MetaInfo.StatusInfo(metaObject.getUuid(), MetaInfo.StatusInfo.Status.CREATED, metaObject.getType(), metaObject.getFullName());
        
        putStatusInfo(status, WASecurityManager.TOKEN);
        if (logger.isDebugEnabled()) {
          logger.debug("Putting MetaObject with URI: " + metaObject.getUri() + " from Database");
        }
        if (logger.isDebugEnabled()) {
          logger.debug(metaObject.getUri());
        }
      }
      else
      {
        logger.error("Version Manager couldn't put the object");
      }
    }
  }
  
  public static MetadataRepository getINSTANCE()
  {
    return INSTANCE;
  }
  
  public static void shutdown()
  {
    MetadataRepository metadataRepository = getINSTANCE();
    String persist = System.getProperty("com.bloom.config.persist");
    assert (persist != null);
    if ((!HazelcastSingleton.isClientMember()) && (persist.equalsIgnoreCase("true"))) {
      metadataRepository.shutDownMetaDataDB();
    }
    metadataRepository.shutDownMDCache();
  }
  
  public boolean shutDownMDCache()
  {
    return this.mdc.shutDown();
  }
  
  public void shutDownMetaDataDB()
  {
    this.mdDBOps.shutdown();
  }
  
  public void putShowStream(MetaInfo.ShowStream showStream, AuthToken token)
    throws MetaDataRepositoryException
  {
    MDConstants.checkNullParams("Can't pass NULL parameters for Action:putShowStream(...) in Meta Data Repository", new Object[] { showStream, token });
    if (HazelcastSingleton.isClientMember()) {
      MDClientOps.getINSTANCE().putShowStream(showStream, token);
    } else {
      this.mdc.put(showStream);
    }
  }
  
  public void putStatusInfo(MetaInfo.StatusInfo statusInfo, AuthToken token)
    throws MetaDataRepositoryException
  {
    MDConstants.checkNullParams("Can't pass NULL parameters for Action:putStatusInfo(...) in Meta Data Repository", new Object[] { statusInfo, token });
    if (HazelcastSingleton.isClientMember()) {
      MDClientOps.getINSTANCE().putStatusInfo(statusInfo, token);
    } else {
      this.mdc.put(statusInfo);
    }
  }
  
  public void putDeploymentInfo(Pair<UUID, UUID> deploymentInfo, AuthToken token)
    throws MetaDataRepositoryException
  {
    MDConstants.checkNullParams("Can't pass NULL parameters for Action:putDeploymentInfo(...) in Meta Data Repository", new Object[] { deploymentInfo, token });
    if (HazelcastSingleton.isClientMember()) {
      MDClientOps.getINSTANCE().putDeploymentInfo(deploymentInfo, token);
    } else {
      this.mdc.put(deploymentInfo);
    }
  }
  
  public void putServer(MetaInfo.Server server, AuthToken token)
    throws MetaDataRepositoryException
  {
    MDConstants.checkNullParams("Can't pass NULL parameters for Action:putDeploymentInfo(...) in Meta Data Repository", new Object[] { server, token });
    this.mdc.put(server);
  }
  
  public void putMetaObject(MetaInfo.MetaObject metaObject, AuthToken token)
    throws MetaDataRepositoryException
  {
    if (HazelcastSingleton.isClientMember())
    {
      MDClientOps.getINSTANCE().putMetaObject(metaObject, token);
    }
    else
    {
      if ((metaObject.namespaceId.equals(MetaInfo.GlobalUUID)) && (!metaObject.type.isGlobal()) && (!token.equals(WASecurityManager.TOKEN))) {
        throw new SecurityException("Unable to create object " + metaObject.toString() + " in Meta Data Repository ");
      }
      if (PermissionUtility.checkPermission(metaObject, ObjectPermission.Action.create, token, true))
      {
        if (this.versionManager.put(metaObject))
        {
          if ((metaObject.getType().isStoreable()) && (this.doPersist) && (!metaObject.getMetaInfoStatus().isAdhoc()))
          {
            boolean stored = this.mdDBOps.store(metaObject);
            if (stored) {
              this.mdc.put(metaObject);
            }
          }
          else
          {
            this.mdc.put(metaObject);
          }
          addToRecentVersion(metaObject.nsName, metaObject.uuid);
        }
        else if (logger.isDebugEnabled())
        {
          logger.debug("Some unexpected problem with Version Manager");
        }
      }
      else {
        throw new SecurityException("Oops, you don't have permission to store " + metaObject.toString() + " in Meta Data Repository ");
      }
    }
  }
  
  public MetaInfo.StatusInfo getStatusInfo(UUID uuid, AuthToken token)
    throws MetaDataRepositoryException
  {
    MDConstants.checkNullParams("Either UUID or Auth Token is NULL", new Object[] { uuid, token });
    if (HazelcastSingleton.isClientMember()) {
      return MDClientOps.getINSTANCE().getStatusInfo(uuid, token);
    }
    return (MetaInfo.StatusInfo)this.mdc.get(null, uuid, null, "status", null, MDConstants.typeOfGet.BY_UUID);
  }
  
  public Collection<UUID> getServersForDeployment(UUID uuid, AuthToken token)
    throws MetaDataRepositoryException
  {
    MDConstants.checkNullParams("Either UUID or Auth Token is NULL", new Object[] { uuid, token });
    if (HazelcastSingleton.isClientMember()) {
      return MDClientOps.getINSTANCE().getServersForDeployment(uuid, token);
    }
    return (Collection)this.mdc.get(null, uuid, null, "serversForDeployment", null, MDConstants.typeOfGet.BY_UUID);
  }
  
  public MetaInfo.MetaObject getServer(String name, AuthToken token)
    throws MetaDataRepositoryException
  {
    MDConstants.checkNullParams("Can't get Server information when Server Name or Authentication Token is NULL\nServer Name: " + name + "\nToken: " + token, new Object[] { name, token });
    Set<MetaInfo.Server> allServers;
    if (HazelcastSingleton.isClientMember()) {
      allServers = MDClientOps.getINSTANCE().getByEntityType(EntityType.SERVER, token);
    } else {
      allServers = getByEntityType(EntityType.SERVER, token);
    }
    for (MetaInfo.MetaObject candidate : allServers) {
      if (name.equalsIgnoreCase(candidate.name)) {
        return candidate;
      }
    }
    return null;
  }
  
  public Integer getMaxClassId()
  {
    return Integer.valueOf(this.mdDBOps == null ? -1 : this.mdDBOps.loadMaxClassId().intValue());
  }
  
  public MetaInfo.MetaObject getMetaObjectByUUID(UUID uuid, AuthToken token)
    throws MetaDataRepositoryException
  {
    MDConstants.checkNullParams("Either UUID or Auth Token is NULL", new Object[] { uuid, token });
    if (HazelcastSingleton.isClientMember())
    {
      MDClientOps mdClientOps = MDClientOps.getINSTANCE();
      return mdClientOps.getMetaObjectByUUID(uuid, token);
    }
    MetaInfo.MetaObject result = (MetaInfo.MetaObject)this.mdc.get(null, uuid, null, null, null, MDConstants.typeOfGet.BY_UUID);
    if (result != null) {
      if ((result instanceof MetaInfo.MetaObject))
      {
        if (!PermissionUtility.checkPermission(result, ObjectPermission.Action.read, token, true)) {
          return null;
        }
      }
      else {
        logger.error("Unexpected Object type : " + result.getClass());
      }
    }
    return result;
  }
  
  public MetaInfo.MetaObject getMetaObjectByName(EntityType entityType, String namespace, String name, Integer version, AuthToken token)
    throws MetaDataRepositoryException, SecurityException
  {
    if (HazelcastSingleton.isClientMember()) {
      return MDClientOps.getINSTANCE().getMetaObjectByName(entityType, namespace, name, version, token);
    }
    if (entityType == null)
    {
      MDConstants.checkNullParams("Unable to get MetaObject by Name, required fields: Name of Object  and Authentication TokenName of the Object: " + name + ", Auth Token: " + name, new Object[] { token });
      
      MetaInfo.MetaObject object = getByUnqualifiedName(null, name, null, token);
      if (object == null) {
        return null;
      }
      return object.getMetaInfoStatus().isDropped() ? null : object;
    }
    MDConstants.checkNullParams("Unable to get MetaObject by Name, required fields: Entity Type, Namespace, Name of Object  and Authentication TokenEntity Type: " + entityType + ", Namespace : " + namespace + ", Name of the Object: " + name + ", Auth Token: " + token, new Object[] { namespace, name, token });
    
    MetaInfo.MetaObject object = getByFullyQualifiedName(entityType, namespace, name, null, token);
    if (object == null) {
      return null;
    }
    return object.getMetaInfoStatus().isDropped() ? null : object;
  }
  
  private MetaInfo.MetaObject getByUnqualifiedName(String namespace, String name, Integer version, AuthToken token)
    throws MetaDataRepositoryException
  {
    Set<MetaInfo.Server> allServers = getByEntityType(EntityType.SERVER, token);
    for (MetaInfo.MetaObject candidate : allServers) {
      if (name.equalsIgnoreCase(candidate.name)) {
        return candidate;
      }
    }
    Set<MetaInfo.MetaObject> allApps = getByEntityType(EntityType.APPLICATION, token);
    for (MetaInfo.MetaObject application : allApps)
    {
      EntityType[] orderedTypes = { EntityType.APPLICATION, EntityType.FLOW, EntityType.SERVER, EntityType.SOURCE, EntityType.STREAM, EntityType.CQ, EntityType.TARGET, EntityType.WACTIONSTORE, EntityType.WINDOW };
      for (EntityType et : orderedTypes)
      {
        MetaInfo.MetaObject metaObject = getByFullyQualifiedName(et, application.getName(), name, null, token);
        if (metaObject != null) {
          return metaObject;
        }
      }
    }
    for (MetaInfo.MetaObject metaObject : allApps)
    {
      MetaInfo.MetaObject candidate = getMetaObjectByUUID(metaObject.getUuid(), token);
      if (name.equalsIgnoreCase(candidate.name)) {
        return candidate;
      }
    }
    Set<MetaInfo.MetaObject> allUsers = getByEntityType(EntityType.USER, token);
    for (MetaInfo.MetaObject candidate : allUsers) {
      if ((candidate != null) && (name.equalsIgnoreCase(candidate.name))) {
        return candidate;
      }
    }
    Set<MetaInfo.Role> allRoles = getByEntityType(EntityType.ROLE, token);
    for (MetaInfo.MetaObject candidate : allRoles) {
      if (name.equalsIgnoreCase(candidate.name)) {
        return candidate;
      }
    }
    return null;
  }
  
  private MetaInfo.MetaObject getByFullyQualifiedName(EntityType eType, String namespace, String name, Integer version, AuthToken token)
    throws MetaDataRepositoryException
  {
    MetaInfo.MetaObject result = null;
    if (version == null)
    {
      version = this.versionManager.getLatestVersionOfMetaObject(eType, namespace, name);
      if (logger.isDebugEnabled()) {
        logger.debug("Version: " + version + " for " + name + " in namespace: " + namespace);
      }
      if (version != null)
      {
        result = (MetaInfo.MetaObject)this.mdc.get(eType, null, namespace, name, version, MDConstants.typeOfGet.BY_NAME);
      }
      else
      {
        if (logger.isDebugEnabled()) {
          logger.debug("Version doesn't exist. Implies a corresponding Object will not be found!!");
        }
        return null;
      }
    }
    else
    {
      result = (MetaInfo.MetaObject)this.mdc.get(eType, null, namespace, name, version, MDConstants.typeOfGet.BY_NAME);
    }
    if ((result == null) && (this.doPersist))
    {
      assert (version != null);
      result = (MetaInfo.MetaObject)this.mdDBOps.get(eType, null, namespace, name, version, MDConstants.typeOfGet.BY_NAME);
      if (result != null)
      {
        this.versionManager.put(result);
        this.mdc.put(result);
      }
    }
    if (result != null) {
      if (!PermissionUtility.checkPermission(result, ObjectPermission.Action.read, token, false)) {
        return null;
      }
    }
    return result;
  }
  
  public Set getByEntityType(EntityType eType, AuthToken token)
    throws MetaDataRepositoryException
  {
    if (HazelcastSingleton.isClientMember()) {
      return MDClientOps.getINSTANCE().getByEntityType(eType, token);
    }
    Set<MetaInfo.MetaObject> cacheResult = null;
    Set<MetaInfo.MetaObject> resultSet = new HashSet();
    cacheResult = (Set)this.mdc.get(eType, null, null, null, null, MDConstants.typeOfGet.BY_ENTITY_TYPE);
    if ((cacheResult == null) && (this.doPersist))
    {
      cacheResult = (Set)this.mdDBOps.get(eType, null, null, null, null, MDConstants.typeOfGet.BY_ENTITY_TYPE);
      if (cacheResult != null) {
        for (MetaInfo.MetaObject metaObject : cacheResult)
        {
          this.versionManager.put(metaObject);
          this.mdc.put(metaObject);
        }
      }
    }
    if (cacheResult != null) {
      for (MetaInfo.MetaObject metaObject : cacheResult) {
        if (PermissionUtility.checkPermission(metaObject, ObjectPermission.Action.read, token, false))
        {
          if (resultSet == null) {
            resultSet = new HashSet();
          }
          resultSet.add(metaObject);
        }
      }
    }
    removeDroppedObject(resultSet);
    return resultSet;
  }
  
  public Set<MetaInfo.MetaObject> getByNameSpace(String namespace, AuthToken token)
    throws MetaDataRepositoryException
  {
    if (HazelcastSingleton.isClientMember()) {
      return MDClientOps.getINSTANCE().getByNameSpace(namespace, token);
    }
    Set<MetaInfo.MetaObject> cacheResult = null;
    Set<MetaInfo.MetaObject> resultSet = null;
    cacheResult = (Set)this.mdc.get(null, null, namespace, null, null, MDConstants.typeOfGet.BY_NAMESPACE);
    if ((cacheResult == null) && (this.doPersist))
    {
      cacheResult = (Set)this.mdDBOps.get(null, null, namespace, null, null, MDConstants.typeOfGet.BY_NAMESPACE);
      if (cacheResult != null) {
        for (MetaInfo.MetaObject metaObject : cacheResult)
        {
          this.versionManager.put(metaObject);
          this.mdc.put(metaObject);
        }
      }
    }
    if (cacheResult != null) {
      for (MetaInfo.MetaObject metaObject : cacheResult) {
        if (PermissionUtility.checkPermission(metaObject, ObjectPermission.Action.read, token, false))
        {
          if (resultSet == null) {
            resultSet = new HashSet();
          }
          resultSet.add(metaObject);
        }
      }
    }
    return resultSet;
  }
  
  public Set<?> getByEntityTypeInNameSpace(EntityType eType, String namespace, AuthToken token)
    throws MetaDataRepositoryException
  {
    if (HazelcastSingleton.isClientMember()) {
      return MDClientOps.getINSTANCE().getByEntityTypeInNameSpace(eType, namespace, token);
    }
    Set<MetaInfo.MetaObject> cacheResult = null;
    Set<MetaInfo.MetaObject> resultSet = null;
    cacheResult = (Set)this.mdc.get(eType, null, namespace, null, null, MDConstants.typeOfGet.BY_NAMESPACE_AND_ENTITY_TYPE);
    if ((cacheResult == null) && (this.doPersist))
    {
      cacheResult = (Set)this.mdDBOps.get(eType, null, namespace, null, null, MDConstants.typeOfGet.BY_NAMESPACE_AND_ENTITY_TYPE);
      if (cacheResult != null) {
        for (MetaInfo.MetaObject metaObject : cacheResult)
        {
          this.versionManager.put(metaObject);
          this.mdc.put(metaObject);
        }
      }
    }
    if (cacheResult != null) {
      for (MetaInfo.MetaObject metaObject : cacheResult) {
        if (PermissionUtility.checkPermission(metaObject, ObjectPermission.Action.read, token, false))
        {
          if (resultSet == null) {
            resultSet = new HashSet();
          }
          resultSet.add(metaObject);
        }
      }
    }
    return resultSet;
  }
  
  public Set<?> getByEntityTypeInApplication(EntityType entityType, String namespace, String name, AuthToken token)
    throws MetaDataRepositoryException
  {
    MetaInfo.MetaObject appMetaObject = getMetaObjectByName(EntityType.APPLICATION, namespace, name, null, token);
    if (appMetaObject != null)
    {
      Set<UUID> uuidsOfObjectsInApp = ((MetaInfo.Flow)appMetaObject).getObjects(entityType);
      Set<MetaInfo.MetaObject> metaObjectsInApp = new HashSet();
      for (UUID uu : uuidsOfObjectsInApp)
      {
        MetaInfo.MetaObject mm;
        if ((mm = getMetaObjectByUUID(uu, token)) != null) {
          metaObjectsInApp.add(mm);
        }
      }
      return metaObjectsInApp;
    }
    return null;
  }
  
  public MetaInfo.MetaObject revert(EntityType eType, String namespace, String name, AuthToken token)
    throws MetaDataRepositoryException
  {
    Integer lastestVersion = this.versionManager.getLatestVersionOfMetaObject(eType, namespace, name);
    MetaInfo.MetaObject metaObject = null;
    if (lastestVersion != null)
    {
      removeMetaObjectByName(eType, namespace, name, lastestVersion, token);
      metaObject = getMetaObjectByName(eType, namespace, name, null, token);
    }
    return metaObject;
  }
  
  public void removeMetaObjectByName(EntityType eType, String namespace, String name, Integer version, AuthToken token)
    throws MetaDataRepositoryException
  {
    MDConstants.checkNullParams("One of the params for needed for removing Meta Object from MetaData Repository is NULL. \nEntityType: " + eType + ", Namespace: " + namespace + ", Name:" + name, new Object[] { eType, namespace, name, token });
    if (HazelcastSingleton.isClientMember())
    {
      MDClientOps.getINSTANCE().removeMetaObjectByName(eType, namespace, name, version, token);
    }
    else
    {
      MetaInfo.MetaObject metaObject = getMetaObjectByName(eType, namespace, name, version, token);
      if (metaObject == null)
      {
        logger.debug("No MetaObject found for: " + name);
        return;
      }
      removeFromRecentVersion(metaObject.nsName, metaObject.uuid);
      if (version == null)
      {
        List<Integer> allVersions = this.versionManager.getAllVersionsOfMetaObject(eType, namespace, name);
        if (allVersions != null)
        {
          for (Integer currentVersion : allVersions)
          {
            Object result = this.mdc.remove(eType, null, namespace, name, currentVersion, MDConstants.typeOfRemove.BY_NAME);
            if ((result != null) && (this.doPersist)) {
              this.mdDBOps.remove(eType, null, namespace, name, currentVersion, MDConstants.typeOfRemove.BY_NAME);
            }
          }
          this.versionManager.removeAllVersions(eType, namespace, name);
        }
      }
      else
      {
        this.versionManager.removeVersion(eType, namespace, name, version);
        this.mdc.remove(eType, null, namespace, name, version, MDConstants.typeOfRemove.BY_NAME);
        if (this.doPersist) {
          this.mdDBOps.remove(eType, null, namespace, name, version, MDConstants.typeOfRemove.BY_NAME);
        }
      }
    }
  }
  
  public void removeStatusInfo(UUID uuid, AuthToken token)
    throws MetaDataRepositoryException
  {
    MDConstants.checkNullParams("Can't remove Object with NULL UUID", new Object[] { uuid, token });
    if (HazelcastSingleton.isClientMember())
    {
      MDClientOps.getINSTANCE().removeStatusInfo(uuid, token);
    }
    else if (token.equals(WASecurityManager.TOKEN))
    {
      MetaInfo.StatusInfo statusInfo = getStatusInfo(uuid, token);
      if (statusInfo != null) {
        this.mdc.remove(null, statusInfo.getOID(), null, "status", null, MDConstants.typeOfRemove.BY_UUID);
      } else if (logger.isDebugEnabled()) {
        logger.debug("No Status Info object found for: " + uuid);
      }
    }
    else
    {
      logger.error("User with token: " + token.toString() + " can't remove a StatusInfo Object");
    }
  }
  
  public void removeDeploymentInfo(Pair<UUID, UUID> deploymentInfo, AuthToken token)
    throws MetaDataRepositoryException
  {
    MDConstants.checkNullParams("Can't remove Object with NULL UUID", new Object[] { deploymentInfo, token });
    if (HazelcastSingleton.isClientMember())
    {
      MDClientOps.getINSTANCE().removeDeploymentInfo(deploymentInfo, token);
    }
    else if (token.equals(WASecurityManager.TOKEN))
    {
      Collection<UUID> allUUIDs = getServersForDeployment((UUID)deploymentInfo.first, token);
      if (allUUIDs != null) {
        this.mdc.remove(null, deploymentInfo, null, null, null, MDConstants.typeOfRemove.BY_UUID);
      } else if (logger.isDebugEnabled()) {
        logger.debug("No Deployment Info object found for: " + deploymentInfo);
      }
    }
    else
    {
      logger.error("User with token: " + token.toString() + " can't remove a StatusInfo Object");
    }
  }
  
  public void removeMetaObjectByUUID(UUID uuid, AuthToken token)
    throws MetaDataRepositoryException
  {
    MDConstants.checkNullParams("Can't remove Object with NULL UUID", new Object[] { uuid, token });
    if (HazelcastSingleton.isClientMember())
    {
      MDClientOps.getINSTANCE().removeMetaObjectByUUID(uuid, token);
    }
    else
    {
      MetaInfo.MetaObject metaObject = getMetaObjectByUUID(uuid, token);
      if (metaObject == null)
      {
        logger.debug("No MetaObject found for: " + uuid);
        return;
      }
      removeFromRecentVersion(metaObject.nsName, metaObject.uuid);
      if (metaObject != null)
      {
        this.versionManager.removeVersion(metaObject.getType(), metaObject.getNsName(), metaObject.getName(), Integer.valueOf(metaObject.version));
        this.mdc.remove(null, metaObject.getUuid(), null, null, null, MDConstants.typeOfRemove.BY_UUID);
        if ((metaObject.getType().isStoreable()) && (this.doPersist)) {
          this.mdDBOps.remove(null, metaObject.getUuid(), null, null, null, MDConstants.typeOfRemove.BY_UUID);
        }
      }
      else if (logger.isDebugEnabled())
      {
        logger.debug("No MetaObject found for: " + uuid);
      }
    }
  }
  
  public void updateMetaObject(MetaInfo.MetaObject object, AuthToken token)
    throws MetaDataRepositoryException
  {
    token = WASecurityManager.TOKEN;
    MDConstants.checkNullParams("Updated Object was NULL", new Object[] { object });
    if (HazelcastSingleton.isClientMember())
    {
      MDClientOps.getINSTANCE().updateMetaObject(object, token);
    }
    else
    {
      ObjectPermission.Action action = ObjectPermission.Action.update;
      if (PermissionUtility.checkPermission(object, action, token, true))
      {
        this.mdc.update(object);
        if (this.doPersist) {
          if ((object.getType().isStoreable()) && (!object.getMetaInfoStatus().isAdhoc())) {
            this.mdDBOps.store(object);
          }
        }
      }
      else
      {
        logger.error("No permission to update: " + object.getFullName());
      }
    }
  }
  
  public String exportMetadataAsJson()
  {
    return this.mdc.exportMetadataAsJson();
  }
  
  public void importMetadataFromJson(String json, Boolean replace)
  {
    this.mdc.importMetadataFromJson(json, replace);
  }
  
  public void dumpAMap(Map<?, ?> map, String mapName)
  {
    System.out.println(mapName + " entries:");
    for (Map.Entry entry : map.entrySet()) {
      System.out.println(entry.getKey() + ": " + entry.getValue());
    }
  }
  
  public Map dumpMaps()
  {
    Map map = new HashMap();
    map.putAll(this.mdc.getUuidToURL());
    map.putAll(this.mdc.getUrlToMetaObject());
    map.putAll(this.mdc.getStatus());
    for (MetaInfo.MetaObject namespaceName : (Set)this.mdc.get(EntityType.NAMESPACE, null, null, null, null, MDConstants.typeOfGet.BY_ENTITY_TYPE))
    {
      ISet<UUID> set = HazelcastSingleton.get().getSet("#" + namespaceName.name);
      for (UUID object : set)
      {
        MetaInfo.MetaObject obj = (MetaInfo.MetaObject)this.mdc.get(null, object, null, null, null, MDConstants.typeOfGet.BY_UUID);
        map.put(object, obj.type + " " + obj.name + " " + obj.version);
      }
    }
    return map;
  }
  
  public boolean removeDGInfoFromServer(MetaInfo.MetaObject metaObject, AuthToken authToken)
    throws MetaDataRepositoryException
  {
    if (HazelcastSingleton.isClientMember()) {
      return MDClientOps.getINSTANCE().removeDGInfoFromServer(metaObject, authToken);
    }
    boolean wasRemoved = false;
    if (Server.server.ServerInfo.deploymentGroupsIDs.contains(metaObject.getUuid()))
    {
      wasRemoved = Server.server.ServerInfo.deploymentGroupsIDs.remove(metaObject.getUuid());
      if (wasRemoved)
      {
        putServer(Server.server.ServerInfo, authToken);
        if (logger.isInfoEnabled()) {
          logger.info("Removed Server" + Server.server.ServerInfo.getName() + " with UUID: " + Server.server.ServerInfo.getUuid() + "from DG: " + metaObject.getName() + " with UUID: " + metaObject.getUuid());
        }
      }
    }
    return wasRemoved;
  }
  
  public void removeDroppedObject(Set<MetaInfo.MetaObject> result)
  {
    for (Iterator<MetaInfo.MetaObject> it = result.iterator(); it.hasNext();)
    {
      MetaInfo.MetaObject metaObject = (MetaInfo.MetaObject)it.next();
      if (metaObject.getMetaInfoStatus().isDropped()) {
        it.remove();
      }
    }
  }
  
  public List<MetaInfo.Flow> getAllApplications(AuthToken token)
  {
    List<MetaInfo.Flow> resultList = new ArrayList();
    try
    {
      Set<MetaInfo.Flow> result = getByEntityType(EntityType.APPLICATION, token);
      if (result != null) {
        resultList.addAll(result);
      }
      Utility.removeDroppedFlow(resultList);
      Utility.removeAdhocNamedQuery(resultList);
    }
    catch (MetaDataRepositoryException e)
    {
      logger.error(e.getMessage());
    }
    return resultList;
  }
  
  public List<MetaInfo.Namespace> getAllNamespaces(AuthToken token)
  {
    Set<MetaInfo.Namespace> result = null;
    try
    {
      result = getByEntityType(EntityType.NAMESPACE, token);
      return new ArrayList(result);
    }
    catch (MetaDataRepositoryException e)
    {
      e.printStackTrace();
    }
    return null;
  }
  
  public Set<EntityType> getPlatformEntitiesByNamespace(String nsName)
  {
    Set<EntityType> entities = new HashSet();
    for (EntityType tt : EntityType.values()) {
      if (nsName.equals("Global"))
      {
        if ((tt.isGlobal()) && (!tt.isSystem())) {
          entities.add(tt);
        }
      }
      else if ((!tt.isSystem()) && (!tt.isGlobal())) {
        entities.add(tt);
      }
    }
    return entities;
  }
  
  private void addStreamDependants(MetaInfo.Stream strm, AuthToken token, Map<UUID, MetaInfo.MetaObject> appObjs)
  {
    if (strm.dataType != null)
    {
      if (appObjs.get(strm.dataType) != null) {
        return;
      }
      addMetaObject(strm.dataType, token, appObjs);
    }
  }
  
  private void addWindowDependants(MetaInfo.Window window, AuthToken token, Map<UUID, MetaInfo.MetaObject> appObjs)
  {
    if (window.stream != null)
    {
      if (appObjs.get(window.stream) != null) {
        return;
      }
      addMetaObject(window.stream, token, appObjs);
    }
  }
  
  private void addCacheDependants(MetaInfo.Cache cache, AuthToken token, Map<UUID, MetaInfo.MetaObject> appObjs)
  {
    if (cache.typename != null)
    {
      if (appObjs.get(cache.typename) != null) {
        return;
      }
      addMetaObject(cache.typename, token, appObjs);
    }
  }
  
  private void addWactionStoreDependants(MetaInfo.WActionStore ws, AuthToken token, Map<UUID, MetaInfo.MetaObject> appObjs)
  {
    if (ws.contextType != null) {
      if (appObjs.get(ws.contextType) == null) {
        addMetaObject(ws.contextType, token, appObjs);
      }
    }
    if (ws.eventTypes != null) {
      for (UUID u : ws.eventTypes) {
        if (appObjs.get(u) == null) {
          addMetaObject(u, token, appObjs);
        }
      }
    }
  }
  
  private void addSourceDependants(MetaInfo.Source src, AuthToken token, Map<UUID, MetaInfo.MetaObject> appObjs)
  {
    if (src.outputStream != null)
    {
      if (appObjs.get(src.outputStream) != null) {
        return;
      }
      addMetaObject(src.outputStream, token, appObjs);
    }
  }
  
  private void addTargetDependants(MetaInfo.Target target, AuthToken token, Map<UUID, MetaInfo.MetaObject> appObjs)
  {
    if (target.inputStream != null)
    {
      if (appObjs.get(target.inputStream) != null) {
        return;
      }
      addMetaObject(target.inputStream, token, appObjs);
    }
  }
  
  private void addMetaObject(UUID objUUID, AuthToken token, Map<UUID, MetaInfo.MetaObject> appObjs)
  {
    MetaInfo.MetaObject obj = (MetaInfo.MetaObject)this.mdc.get(null, objUUID, null, null, null, MDConstants.typeOfGet.BY_UUID);
    if (obj == null) {
      return;
    }
    appObjs.put(obj.uuid, obj);
    switch (obj.type)
    {
    case SOURCE: 
      addSourceDependants((MetaInfo.Source)obj, token, appObjs);
      break;
    case CQ: 
      addCQDependants((MetaInfo.CQ)obj, token, appObjs);
      break;
    case TARGET: 
      addTargetDependants((MetaInfo.Target)obj, token, appObjs);
      break;
    case FLOW: 
      addFlowDependants((MetaInfo.Flow)obj, token, appObjs);
      break;
    case STREAM: 
      addStreamDependants((MetaInfo.Stream)obj, token, appObjs);
      break;
    case WINDOW: 
      addWindowDependants((MetaInfo.Window)obj, token, appObjs);
      break;
    case CACHE: 
      addCacheDependants((MetaInfo.Cache)obj, token, appObjs);
      break;
    case WACTIONSTORE: 
      addWactionStoreDependants((MetaInfo.WActionStore)obj, token, appObjs);
      
      break;
    case TYPE: 
      break;
    }
  }
  
  private void addFlowDependants(MetaInfo.Flow flow, AuthToken token, Map<UUID, MetaInfo.MetaObject> appObjs)
  {
    for (EntityType cType : flow.getObjectTypes())
    {
      Set<UUID> flowObjs = flow.getObjects(cType);
      for (UUID objectid : flowObjs) {
        if (appObjs.get(objectid) == null) {
          addMetaObject(objectid, token, appObjs);
        }
      }
    }
  }
  
  private void addCQDependants(MetaInfo.CQ cq, AuthToken token, Map<UUID, MetaInfo.MetaObject> appObjs)
  {
    if (cq.stream != null) {
      if (appObjs.get(cq.stream) == null) {
        addMetaObject(cq.stream, token, appObjs);
      }
    }
    CQExecutionPlan plan = cq.plan;
    if (plan != null) {
      for (UUID ds : plan.getDataSources()) {
        if (appObjs.get(ds) == null) {
          addMetaObject(ds, token, appObjs);
        }
      }
    }
  }
  
  private Map<UUID, MetaInfo.MetaObject> getApplicationDependencyGraph(String nsName, String appName, boolean deepTraverse, AuthToken token)
  {
    Map<UUID, MetaInfo.MetaObject> appObjects = new HashMap();
    
    MetaInfo.Flow app = null;
    try
    {
      app = (MetaInfo.Flow)getMetaObjectByName(EntityType.APPLICATION, nsName, appName, null, WASecurityManager.TOKEN);
    }
    catch (MetaDataRepositoryException e)
    {
      logger.error(e.getMessage());
    }
    if (app != null) {
      for (EntityType type : app.getObjectTypes())
      {
        Set<UUID> childObjs = app.getObjects(type);
        for (UUID objUid : childObjs) {
          if (appObjects.get(objUid) == null) {
            if (!deepTraverse)
            {
              MetaInfo.MetaObject obj = null;
              try
              {
                obj = getMetaObjectByUUID(objUid, WASecurityManager.TOKEN);
              }
              catch (MetaDataRepositoryException e)
              {
                logger.error(e.getMessage());
              }
              if (obj != null) {
                appObjects.put(obj.uuid, obj);
              }
            }
            else
            {
              addMetaObject(objUid, token, appObjects);
            }
          }
        }
      }
    }
    return appObjects;
  }
  
  Comparator<UUID> timestampComparator = new Comparator<UUID>()
  {
    public int compare(UUID o1, UUID o2)
    {
      if (o1.time > o2.time) {
        return 1;
      }
      return -1;
    }
  };
  
  public String getApplicationTQL(String nsName, String appName, boolean deepTraverse, AuthToken token)
    throws MetaDataRepositoryException
  {
    StringBuffer tql = new StringBuffer();
    MetaInfo.Flow app = (MetaInfo.Flow)getMetaObjectByName(EntityType.APPLICATION, nsName, appName, null, token);
    if (app == null)
    {
      logger.warn("App " + nsName + "." + appName + " is null");
      return tql.toString();
    }
    if (app.importStatements != null)
    {
      for (String importStmt : app.importStatements) {
        tql.append("IMPORT " + importStmt + ";\n");
      }
      tql.append("\n");
    }
    try
    {
      List<Pair<MetaInfo.MetaObject, MetaInfo.Flow>> ll = app.exportTQL(getINSTANCE(), token);
      String recovClause;
      if (app.recoveryPeriod > 0L) {
        recovClause = " RECOVERY " + new Interval(1000000L * app.recoveryPeriod).toHumanReadable() + " INTERVAL";
      } else {
        recovClause = "";
      }
      String encryptionClause;
      if (app.encrypted) {
        encryptionClause = " WITH ENCRYPTION";
      } else {
        encryptionClause = "";
      }
      tql.append("CREATE APPLICATION " + app.name + encryptionClause + recovClause);
      if ((app.ehandlers != null) && (!app.ehandlers.isEmpty()))
      {
        tql.append(" EXCEPTIONHANDLER (");
        
        Iterator<Map.Entry<String, Object>> it = app.ehandlers.entrySet().iterator();
        while (it.hasNext())
        {
          Map.Entry<String, Object> ehandler = (Map.Entry)it.next();
          if (ehandler.getValue() != null)
          {
            tql.append(String.format("%s: '%s'", new Object[] { ehandler.getKey(), ehandler.getValue() }));
            if (it.hasNext()) {
              tql.append(", ");
            }
          }
        }
        tql.append(") ");
      }
      tql.append(";\n\n");
      
      String currentFlow = null;
      Set<String> seenFlow = new HashSet();
      seenFlow.add(app.name);
      for (Pair<MetaInfo.MetaObject, MetaInfo.Flow> pair : ll) {
        if (((MetaInfo.MetaObject)pair.first).getSourceText() != null)
        {
          boolean firstTimeSeeing = false;
          if (!seenFlow.contains(((MetaInfo.Flow)pair.second).name))
          {
            seenFlow.add(((MetaInfo.Flow)pair.second).name);
            if (currentFlow != null) {
              tql.append("END FLOW " + currentFlow + ";\n\n");
            }
            tql.append("CREATE FLOW " + ((MetaInfo.Flow)pair.second).name + ";\n\n");
            currentFlow = ((MetaInfo.Flow)pair.second).name;
            firstTimeSeeing = true;
          }
          if (!firstTimeSeeing)
          {
            if ((((MetaInfo.Flow)pair.second).type == EntityType.FLOW) && (currentFlow == null))
            {
              tql.append("ALTER FLOW " + ((MetaInfo.Flow)pair.second).name + ";\n\n");
              currentFlow = ((MetaInfo.Flow)pair.second).name;
            }
            else if ((((MetaInfo.Flow)pair.second).type == EntityType.FLOW) && (!((MetaInfo.Flow)pair.second).name.equalsIgnoreCase(currentFlow)))
            {
              tql.append("END FLOW " + currentFlow + ";\n\n");
              tql.append("ALTER FLOW " + ((MetaInfo.Flow)pair.second).name + ";\n\n");
              currentFlow = ((MetaInfo.Flow)pair.second).name;
            }
            if (((MetaInfo.Flow)pair.second).type == EntityType.APPLICATION)
            {
              if (currentFlow != null) {
                tql.append("END FLOW " + currentFlow + ";\n\n");
              }
              currentFlow = null;
            }
          }
          printObject(tql, (MetaInfo.MetaObject)pair.first, token);
        }
      }
      if ((currentFlow != null) && (!currentFlow.isEmpty())) {
        tql.append("END FLOW " + currentFlow + ";\n\n");
      }
      tql.append("END APPLICATION " + app.name + ";\n\n");
    }
    catch (Exception e)
    {
      logger.error(e);
    }
    return tql.toString();
  }
  
  private void printObject(StringBuffer tql, MetaInfo.MetaObject obj, AuthToken token)
    throws MetaDataRepositoryException
  {
    String temp = obj.getSourceText();
    if ((temp != null) && (!temp.isEmpty())) {
      if (obj.type == EntityType.SOURCE)
      {
        MetaInfo.Source sourceMetaObject = (MetaInfo.Source)obj;
        if (sourceMetaObject.outputClauses.size() > 1)
        {
          String sourceText = temp;
          try
          {
            List<Stmt> stmt = new Grammar(new Lexer(sourceText + ";")).parseStmt(false);
            CreateSourceOrTargetStmt sourceStmt = (CreateSourceOrTargetStmt)stmt.get(0);
            String realOutput;
            if (sourceStmt.parserOrFormatter == null) {
              realOutput = Utility.createSourceStatementText(sourceMetaObject.getName(), Boolean.valueOf(false), sourceStmt.srcOrDest.getAdapterTypeName(), sourceStmt.srcOrDest.getProps(), null, null, sourceMetaObject.outputClauses);
            } else {
              realOutput = Utility.createSourceStatementText(sourceMetaObject.getName(), Boolean.valueOf(false), sourceStmt.srcOrDest.getAdapterTypeName(), sourceStmt.srcOrDest.getProps(), sourceStmt.parserOrFormatter.getAdapterTypeName(), sourceStmt.parserOrFormatter.getProps(), sourceMetaObject.outputClauses);
            }
            sourceText = realOutput;
          }
          catch (Exception e)
          {
            if (logger.isDebugEnabled()) {
              logger.debug(e.getMessage(), e);
            }
            sourceText = StringUtils.substringBefore(sourceText, "OUTPUT TO");
            sourceText = Utility.appendOutputClause(sourceText, sourceMetaObject.outputClauses);
          }
          finally
          {
            tql.append(sourceText).append(";\n\n");
          }
        }
        else
        {
          tql.append(temp).append(";\n\n");
        }
      }
      else if (obj.type == EntityType.TARGET)
      {
        MetaInfo.Target targetMetaObject = (MetaInfo.Target)obj;
        if (targetMetaObject.isSubscription())
        {
          MetaInfo.Stream stream = (MetaInfo.Stream)getMetaObjectByUUID(targetMetaObject.inputStream, token);
          tql.append(Utility.createSubscriptionStatementText(targetMetaObject.name, Boolean.valueOf(false), Utility.convertAdapterClassToName(targetMetaObject.adapterClassName), Utility.makePropList(targetMetaObject.properties), null, null, stream.getFullName()) + ";\n\n");
        }
        else
        {
          tql.append(temp).append(";\n\n");
        }
      }
      else
      {
        tql.append(temp).append(";\n\n");
      }
    }
  }
  
  private void drillDownTql(StringBuffer tql, AuthToken token, MetaInfo.Flow app)
    throws MetaDataRepositoryException
  {
    List<UUID> appObjects = app.getDependencies();
    Collections.sort(appObjects, this.timestampComparator);
    for (UUID uuid : appObjects)
    {
      MetaInfo.MetaObject obj = getMetaObjectByUUID(uuid, token);
      if (obj.type == EntityType.FLOW) {
        drillDownTql(tql, token, (MetaInfo.Flow)obj);
      } else {
        printObject(tql, obj, token);
      }
    }
  }
  
  public List<MetaInfo.MetaObject> getAllObjectsInApp(String nsName, String appName, boolean deepTraverse, AuthToken token)
  {
    List<MetaInfo.MetaObject> result = new ArrayList();
    
    Map<UUID, MetaInfo.MetaObject> objs = getApplicationDependencyGraph(nsName, appName, deepTraverse, token);
    for (UUID uid : objs.keySet()) {
      result.add(objs.get(uid));
    }
    return result;
  }
  
  public List<MetaInfo.MetaObject> getAllObjectsInNamespace(String nsName, AuthToken token)
  {
    try
    {
      Set<MetaInfo.MetaObject> result = getByNameSpace(nsName, token);
      List<MetaInfo.MetaObject> metaObjectList = new ArrayList(result);
      Utility.removeDroppedObjects(metaObjectList);
      return metaObjectList;
    }
    catch (MetaDataRepositoryException e)
    {
      logger.error(e.getMessage());
    }
    return null;
  }
  
  public List<MetaInfo.MetaObject> getAllVersionsObjectsInNamespace(String nsName, AuthToken token)
  {
    List<MetaInfo.MetaObject> result = (List)this.mdc.get(null, null, nsName, null, null, MDConstants.typeOfGet.BY_NAMESPACE);
    
    return result;
  }
  
  public boolean contains(MDConstants.typeOfRemove contains, UUID uuid, String url)
  {
    return false;
  }
  
  public boolean clear(boolean cleanDB)
  {
    if (cleanDB) {
      this.mdDBOps.clear();
    }
    this.versionManager.clear();
    return this.mdc.clear();
  }
  
  public String registerListerForDeploymentInfo(EntryListener<UUID, UUID> entryListener)
  {
    return this.mdc.registerListerForDeploymentInfo(entryListener);
  }
  
  public String registerListenerForShowStream(MessageListener<MetaInfo.ShowStream> messageListener)
  {
    return this.mdc.registerListenerForShowStream(messageListener);
  }
  
  public String registerListenerForStatusInfo(EntryListener<UUID, MetaInfo.StatusInfo> entryListener)
  {
    return this.mdc.registerListenerForStatusInfo(entryListener);
  }
  
  public String registerListenerForMetaObject(EntryListener<String, MetaInfo.MetaObject> entryListener)
  {
    return this.mdc.registerListenerForMetaObject(entryListener);
  }
  
  public String registerListenerForServer(EntryListener<UUID, MetaInfo.Server> entryListener)
  {
    return this.mdc.registerListenerForServer(entryListener);
  }
  
  public void removeListerForDeploymentInfo(String regId)
  {
    this.mdc.removeListerForDeploymentInfo(regId);
  }
  
  public void removeListenerForShowStream(String regId)
  {
    this.mdc.removeListenerForShowStream(regId);
  }
  
  public void removeListenerForStatusInfo(String regId)
  {
    this.mdc.removeListenerForStatusInfo(regId);
  }
  
  public void removeListenerForMetaObject(String regId)
  {
    this.mdc.removeListenerForMetaObject(regId);
  }
  
  public void removeListenerForServer(String regId)
  {
    this.mdc.removeListenerForServer(regId);
  }
  
  public void addToRecentVersion(String namespaceName, UUID objectName)
  {
    HazelcastSingleton.get().getSet("#" + namespaceName).add(objectName);
  }
  
  public void removeFromRecentVersion(String namespaceName, UUID objectName)
  {
    HazelcastSingleton.get().getSet("#" + namespaceName).remove(objectName);
  }
  
  public void putAppCheckpointForFlow(Pair<UUID, Position> positionInfo, AuthToken token)
    throws MetaDataRepositoryException
  {
    if (HazelcastSingleton.isClientMember()) {
      MDClientOps.getINSTANCE().putAppCheckpointForFlow(positionInfo, token);
    } else {
      StatusDataStore.getInstance().putAppCheckpoint((UUID)positionInfo.first, (Position)positionInfo.second);
    }
  }
  
  public Position getAppCheckpointForFlow(UUID sourceUUID, AuthToken token)
    throws MetaDataRepositoryException
  {
    if (HazelcastSingleton.isClientMember()) {
      return MDClientOps.getINSTANCE().getAppCheckpointForFlow(sourceUUID, token);
    }
    return StatusDataStore.getInstance().getAppCheckpoint(sourceUUID);
  }
}
