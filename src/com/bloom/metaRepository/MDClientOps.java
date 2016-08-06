package com.bloom.metaRepository;

import com.bloom.drop.DropMetaObject;
import com.bloom.drop.DropMetaObject.DropRule;
import com.bloom.runtime.Context;
import com.bloom.runtime.DistributedExecutionManager;
import com.bloom.runtime.Pair;
import com.bloom.runtime.Server;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.ShowStream;
import com.bloom.runtime.meta.MetaInfo.StatusInfo;
import com.bloom.security.WASecurityManager;
import com.bloom.uuid.AuthToken;
import com.bloom.uuid.UUID;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.MessageListener;
import com.bloom.recovery.Position;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;

public class MDClientOps
  implements MDRepository
{
  private static Logger logger = Logger.getLogger(MDClientOps.class);
  private final String clusterName;
  private static MDClientOps INSTANCE = new MDClientOps(HazelcastSingleton.get().getName());
  
  public static MDClientOps getINSTANCE()
  {
    return INSTANCE;
  }
  
  public MDClientOps(String clusterName)
  {
    this.clusterName = clusterName;
  }
  
  public void initialize() {}
  
  public Collection executeRemoteOperation(RemoteCall action)
  {
    HazelcastInstance hz = HazelcastSingleton.get(this.clusterName);
    Collection results = new ArrayList();
    results.add(DistributedExecutionManager.execOnAny(hz, action));
    return results;
  }
  
  public void putShowStream(MetaInfo.ShowStream showStream, AuthToken token)
    throws MetaDataRepositoryException
  {
    Put showStreamObject = new Put(showStream, token, MDConstants.typeOfPut.SHOWSTREAMOBJECT);
    executeRemoteOperation(showStreamObject);
  }
  
  public void putStatusInfo(MetaInfo.StatusInfo statusInfo, AuthToken token)
    throws MetaDataRepositoryException
  {
    Put putStatusInfo = new Put(statusInfo, token, MDConstants.typeOfPut.STATUS_INFO);
    executeRemoteOperation(putStatusInfo);
  }
  
  public void putDeploymentInfo(Pair<UUID, UUID> deploymentInfo, AuthToken token)
    throws MetaDataRepositoryException
  {
    Put putDeploymentInfo = new Put(deploymentInfo, token, MDConstants.typeOfPut.DEPLOYMENT_INFO);
    executeRemoteOperation(putDeploymentInfo);
  }
  
  public void putServer(MetaInfo.Server server, AuthToken token)
    throws MetaDataRepositoryException
  {
    Put serverObject = new Put(server, token, MDConstants.typeOfPut.SERVERMETAOBJECT);
    executeRemoteOperation(serverObject);
  }
  
  public void putMetaObject(MetaInfo.MetaObject metaObject, AuthToken token)
    throws MetaDataRepositoryException
  {
    Put putMetaObject = new Put(metaObject, token, MDConstants.typeOfPut.METAOBJECT);
    executeRemoteOperation(putMetaObject);
  }
  
  public void putAppCheckpointForFlow(Pair<UUID, Position> positioninfo, AuthToken token)
    throws MetaDataRepositoryException
  {
    Put putPositionInfo = new Put(positioninfo, token, MDConstants.typeOfPut.POSITIONINFO);
    executeRemoteOperation(putPositionInfo);
  }
  
  public static class Put
    implements RemoteCall<Object>
  {
    private static final long serialVersionUID = -7423388614430552934L;
    Object obj;
    AuthToken token;
    MDConstants.typeOfPut typeOfPut;
    
    public Put(Object obj, AuthToken token, MDConstants.typeOfPut typeOfPut)
    {
      this.obj = obj;
      this.token = token;
      this.typeOfPut = typeOfPut;
    }
    
    public Object call()
      throws MetaDataRepositoryException
    {
      assert (Server.server != null);
      MDRepository metadataRepository = MetadataRepository.getINSTANCE();
      switch (this.typeOfPut.ordinal())
      {
      case 1: 
        metadataRepository.putStatusInfo((MetaInfo.StatusInfo)this.obj, this.token);
        break;
      case 2: 
        metadataRepository.putAppCheckpointForFlow((Pair)this.obj, this.token);
        break;
      case 3: 
        metadataRepository.putDeploymentInfo((Pair)this.obj, this.token);
        break;
      case 4: 
        metadataRepository.putShowStream((MetaInfo.ShowStream)this.obj, this.token);
        break;
      case 5: 
        metadataRepository.putMetaObject((MetaInfo.MetaObject)this.obj, this.token);
        break;
      case 6: 
        metadataRepository.putServer((MetaInfo.Server)this.obj, this.token);
      }
      return null;
    }
  }
  
  public MetaInfo.MetaObject getMetaObjectByUUID(UUID uuid, AuthToken token)
    throws MetaDataRepositoryException
  {
    Get action = new Get(null, uuid, null, null, null, MDConstants.typeOfGet.BY_UUID, token);
    Collection<Object> resultSet = executeRemoteOperation(action);
    return (MetaInfo.MetaObject)resultSet.toArray()[0];
  }
  
  public MetaInfo.MetaObject getMetaObjectByName(EntityType entityType, String namespace, String name, Integer version, AuthToken token)
    throws MetaDataRepositoryException
  {
    Get action = new Get(entityType, null, namespace, name, version, MDConstants.typeOfGet.BY_NAME, token);
    Collection<Object> resultSet = executeRemoteOperation(action);
    return (MetaInfo.MetaObject)resultSet.toArray()[0];
  }
  
  public Set getByEntityType(EntityType entityType, AuthToken token)
    throws MetaDataRepositoryException
  {
    Get action = new Get(entityType, null, null, null, null, MDConstants.typeOfGet.BY_ENTITY_TYPE, token);
    Collection<Object> resultSet = executeRemoteOperation(action);
    return (Set)resultSet.toArray()[0];
  }
  
  public Set<MetaInfo.MetaObject> getByNameSpace(String namespace, AuthToken token)
    throws MetaDataRepositoryException
  {
    Get action = new Get(null, null, namespace, null, null, MDConstants.typeOfGet.BY_NAMESPACE, token);
    Collection<Object> resultSet = executeRemoteOperation(action);
    return (Set)resultSet.toArray()[0];
  }
  
  public Set<?> getByEntityTypeInNameSpace(EntityType eType, String namespace, AuthToken token)
    throws MetaDataRepositoryException
  {
    Get action = new Get(eType, null, namespace, null, null, MDConstants.typeOfGet.BY_NAMESPACE_AND_ENTITY_TYPE, token);
    Collection<Object> resultSet = executeRemoteOperation(action);
    return (Set)resultSet.toArray()[0];
  }
  
  public Set<?> getByEntityTypeInApplication(EntityType entityType, String namespace, String name, AuthToken token)
    throws MetaDataRepositoryException
  {
    return null;
  }
  
  public MetaInfo.StatusInfo getStatusInfo(UUID uuid, AuthToken token)
    throws MetaDataRepositoryException
  {
    Get statusInfo = new Get(null, uuid, null, null, null, MDConstants.typeOfGet.BY_STATUSINFO, token);
    Collection<MetaInfo.StatusInfo> collection = executeRemoteOperation(statusInfo);
    return (MetaInfo.StatusInfo)collection.toArray()[0];
  }
  
  public Collection<UUID> getServersForDeployment(UUID uuid, AuthToken token)
    throws MetaDataRepositoryException
  {
    Get serversForDeployement = new Get(null, uuid, null, null, null, MDConstants.typeOfGet.BY_SERVERFORDEPLOYMENT, token);
    Collection<UUID> collection = executeRemoteOperation(serversForDeployement);
    return collection;
  }
  
  public MetaInfo.MetaObject getServer(String name, AuthToken token)
    throws MetaDataRepositoryException
  {
    return null;
  }
  
  public Integer getMaxClassId()
  {
    return null;
  }
  
  public Position getAppCheckpointForFlow(UUID flowUUID, AuthToken token)
    throws MetaDataRepositoryException
  {
    Get getPosition = new Get(EntityType.POSITION, flowUUID, null, null, null, MDConstants.typeOfGet.BY_POSITIONINFO, token);
    Collection<Position> collection = executeRemoteOperation(getPosition);
    return (Position)collection.toArray()[0];
  }
  
  public static class Get
    implements RemoteCall<Object>
  {
    private static final long serialVersionUID = -7423381614430552934L;
    EntityType eType;
    UUID uuid;
    String namespace;
    String name;
    Integer version;
    MDConstants.typeOfGet get;
    AuthToken token;
    
    public Get(EntityType eType, UUID uuid, String namespace, String name, Integer version, MDConstants.typeOfGet get, AuthToken token)
    {
      this.eType = eType;
      this.uuid = uuid;
      this.namespace = namespace;
      this.name = name;
      this.version = version;
      this.get = get;
      this.token = token;
    }
    
    public Object call()
      throws MetaDataRepositoryException
    {
      assert (Server.server != null);
      Object result = null;
      MDRepository mdRepository = MetadataRepository.getINSTANCE();
      switch (this.get.ordinal())
      {
      case 1: 
        result = mdRepository.getMetaObjectByUUID(this.uuid, this.token);
        break;
      case 2: 
        result = mdRepository.getByEntityType(this.eType, this.token);
        break;
      case 3: 
        result = mdRepository.getByNameSpace(this.namespace, this.token);
        break;
      case 4: 
        result = mdRepository.getByEntityTypeInNameSpace(this.eType, this.namespace, this.token);
        break;
      case 5: 
        result = mdRepository.getMetaObjectByName(this.eType, this.namespace, this.name, this.version, this.token);
        break;
      case 6: 
        result = mdRepository.getStatusInfo(this.uuid, this.token);
        break;
      case 7: 
        result = mdRepository.getServersForDeployment(this.uuid, this.token);
        break;
      case 8: 
        result = mdRepository.getAppCheckpointForFlow(this.uuid, this.token);
        break;
      }
      return result;
    }
  }
  
  public MetaInfo.MetaObject revert(EntityType eType, String namespace, String name, AuthToken token)
    throws MetaDataRepositoryException
  {
    return null;
  }
  
  public void removeMetaObjectByName(EntityType eType, String namespace, String name, Integer version, AuthToken token)
    throws MetaDataRepositoryException
  {
    Removal action = new Removal(eType, null, namespace, name, version, MDConstants.typeOfRemove.BY_NAME, token);
    executeRemoteOperation(action);
  }
  
  public void removeStatusInfo(UUID uuid, AuthToken token)
    throws MetaDataRepositoryException
  {
    Removal removeStatusInfo = new Removal(null, uuid, null, null, null, MDConstants.typeOfRemove.STATUS_INFO, token);
    executeRemoteOperation(removeStatusInfo);
  }
  
  public void removeDeploymentInfo(Pair<UUID, UUID> deploymentInfo, AuthToken token)
    throws MetaDataRepositoryException
  {
    Removal removeDeploymentInfo = new Removal(null, deploymentInfo, null, null, null, MDConstants.typeOfRemove.DEPLOYMENT_INFO, token);
    executeRemoteOperation(removeDeploymentInfo);
  }
  
  public void removeMetaObjectByUUID(UUID uuid, AuthToken token)
    throws MetaDataRepositoryException
  {
    Removal removeMetaObjectByUUID = new Removal(null, uuid, null, null, null, MDConstants.typeOfRemove.BY_UUID, token);
    executeRemoteOperation(removeMetaObjectByUUID);
  }
  
  public static class Removal
    implements RemoteCall<Object>
  {
    private static final long serialVersionUID = -7423381614430552934L;
    EntityType eType;
    Object uuid;
    String namespace;
    String name;
    Integer version;
    MDConstants.typeOfRemove get;
    AuthToken token;
    
    public Removal(EntityType eType, Object uuid, String namespace, String name, Integer version, MDConstants.typeOfRemove get, AuthToken token)
    {
      this.eType = eType;
      this.uuid = uuid;
      this.namespace = namespace;
      this.name = name;
      this.version = version;
      this.get = get;
      this.token = token;
    }
    
    public Object call()
      throws MetaDataRepositoryException
    {
      assert (Server.server != null);
      MDRepository mdRepository = MetadataRepository.getINSTANCE();
      switch (this.get.ordinal())
      {
      case 1: 
        mdRepository.removeMetaObjectByUUID((UUID)this.uuid, this.token);
        break;
      case 2: 
        mdRepository.removeMetaObjectByName(this.eType, this.namespace, this.name, this.version, this.token);
        break;
      case 3: 
        mdRepository.removeStatusInfo((UUID)this.uuid, this.token);
        break;
      case 4: 
        mdRepository.removeDeploymentInfo((Pair)this.uuid, this.token);
      }
      return null;
    }
  }
  
  public void updateMetaObject(MetaInfo.MetaObject object, AuthToken token)
    throws MetaDataRepositoryException
  {
    Update update = new Update(object, token);
    executeRemoteOperation(update);
  }
  
  public static class Update
    implements RemoteCall<Object>
  {
    private static final long serialVersionUID = -123436876844552934L;
    MetaInfo.MetaObject object;
    AuthToken token;
    
    public Update(MetaInfo.MetaObject object, AuthToken token)
    {
      this.object = object;
      this.token = token;
    }
    
    public Object call()
      throws Exception
    {
      assert (Server.server != null);
      MDRepository mdRepository = MetadataRepository.getINSTANCE();
      mdRepository.updateMetaObject(this.object, this.token);
      return null;
    }
  }
  
  public boolean contains(MDConstants.typeOfRemove contains, UUID uuid, String url)
  {
    return false;
  }
  
  public boolean clear(boolean cleanDB)
  {
    HazelcastInstance hz = HazelcastSingleton.get(this.clusterName);
    Clear action = new Clear();
    Object cleared = DistributedExecutionManager.execOnAny(hz, action);
    return ((Boolean)cleared).booleanValue();
  }
  
  public String registerListerForDeploymentInfo(EntryListener<UUID, UUID> entryListener)
  {
    return null;
  }
  
  public String registerListenerForShowStream(MessageListener<MetaInfo.ShowStream> messageListener)
  {
    return null;
  }
  
  public String registerListenerForStatusInfo(EntryListener<UUID, MetaInfo.StatusInfo> entryListener)
  {
    return null;
  }
  
  public String registerListenerForMetaObject(EntryListener<String, MetaInfo.MetaObject> entryListener)
  {
    return null;
  }
  
  public String registerListenerForServer(EntryListener<UUID, MetaInfo.Server> entryListener)
  {
    return null;
  }
  
  public void removeListerForDeploymentInfo(String regId) {}
  
  public void removeListenerForShowStream(String regId) {}
  
  public void removeListenerForStatusInfo(String regId) {}
  
  public void removeListenerForMetaObject(String regId) {}
  
  public void removeListenerForServer(String regId) {}
  
  public String exportMetadataAsJson()
  {
    return null;
  }
  
  public void importMetadataFromJson(String json, Boolean replace) {}
  
  public Map dumpMaps()
  {
    HazelcastInstance hz = HazelcastSingleton.get(this.clusterName);
    Dump dump = new Dump();
    Object result = DistributedExecutionManager.execOnAny(hz, dump);
    return (Map)result;
  }
  
  public boolean removeDGInfoFromServer(MetaInfo.MetaObject metaObject, AuthToken authToken)
  {
    HazelcastInstance hz = HazelcastSingleton.get(this.clusterName);
    Set<Member> srvs = DistributedExecutionManager.getAllServers(hz);
    if (!srvs.isEmpty())
    {
      RemoveDGInfoFromServerMetaObject rmDG = new RemoveDGInfoFromServerMetaObject(metaObject, authToken);
      DistributedExecutionManager.exec(hz, rmDG, srvs);
    }
    return true;
  }
  
  public AuthToken authenticate(String username, String password)
  {
    return authenticate(username, password, null, null);
  }
  
  public AuthToken authenticate(String username, String password, String clientId, String type)
  {
    HazelcastInstance hz = HazelcastSingleton.get(this.clusterName);
    Authentication authentication = new Authentication(username, password, clientId, type);
    Object result = DistributedExecutionManager.execOnAny(hz, authentication);
    return (AuthToken)result;
  }
  
  public List<String> drop(String objectName, EntityType objectType, DropMetaObject.DropRule dropRule, AuthToken token)
  {
    HazelcastInstance hz = HazelcastSingleton.get(this.clusterName);
    Drop drop = new Drop(objectName, objectType, dropRule, token);
    Object dropped = DistributedExecutionManager.execOnAny(hz, drop);
    return (List)dropped;
  }
  
  public static class Drop
    implements RemoteCall<Object>
  {
    String objectName;
    EntityType entityType;
    DropMetaObject.DropRule dropRule;
    AuthToken token;
    
    public Drop(String objectName, EntityType entityType, DropMetaObject.DropRule dropRule, AuthToken token)
    {
      this.objectName = objectName;
      this.entityType = entityType;
      this.dropRule = dropRule;
      this.token = token;
    }
    
    public Object call()
      throws Exception
    {
      assert (Server.server != null);
      Context context = Context.createContext(this.token);
      return context.dropObject(this.objectName, this.entityType, this.dropRule);
    }
  }
  
  public static class Clear
    implements RemoteCall<Object>
  {
    private static final long serialVersionUID = -74133816144552934L;
    
    public Object call()
      throws Exception
    {
      assert (Server.server != null);
      MDRepository metadataRepository = MetadataRepository.getINSTANCE();
      return Boolean.valueOf(metadataRepository.clear(false));
    }
  }
  
  public static class Dump
    implements RemoteCall<Object>
  {
    private static final long serialVersionUID = -74133816144550074L;
    
    public Object call()
      throws Exception
    {
      assert (Server.server != null);
      MDRepository metadataRepository = MetadataRepository.getINSTANCE();
      return metadataRepository.dumpMaps();
    }
  }
  
  public static class Authentication
    implements RemoteCall<Object>
  {
    private static final long serialVersionUID = -44213816144550014L;
    String username;
    String password;
    String clientId;
    String type = null;
    
    public Authentication(String username, String password, String clientId, String type)
    {
      this.username = username;
      this.password = password;
      this.clientId = clientId;
      this.type = type;
    }
    
    public Object call()
      throws Exception
    {
      assert (Server.server != null);
      WASecurityManager securityManager = WASecurityManager.get();
      return securityManager.authenticate(this.username, this.password, this.clientId, this.type);
    }
  }
  
  public static class RemoveDGInfoFromServerMetaObject
    implements RemoteCall<Object>
  {
    private static final long serialVersionUID = -44213816144014L;
    public AuthToken token;
    public MetaInfo.MetaObject metaObject;
    
    public RemoveDGInfoFromServerMetaObject(MetaInfo.MetaObject metaObject, AuthToken token)
    {
      this.metaObject = metaObject;
      this.token = token;
    }
    
    public Object call()
      throws Exception
    {
      return Boolean.valueOf(MetadataRepository.getINSTANCE().removeDGInfoFromServer(this.metaObject, this.token));
    }
  }
}
