package com.bloom.metaRepository;

import com.bloom.runtime.Pair;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.Server;
import com.bloom.runtime.meta.MetaInfo.ShowStream;
import com.bloom.runtime.meta.MetaInfo.StatusInfo;
import com.bloom.uuid.AuthToken;
import com.bloom.uuid.UUID;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.MessageListener;
import com.bloom.recovery.Position;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public abstract interface MDRepository
{
  public abstract void initialize()
    throws MetaDataRepositoryException;
  
  public abstract void putShowStream(MetaInfo.ShowStream paramShowStream, AuthToken paramAuthToken)
    throws MetaDataRepositoryException;
  
  public abstract void putStatusInfo(MetaInfo.StatusInfo paramStatusInfo, AuthToken paramAuthToken)
    throws MetaDataRepositoryException;
  
  public abstract void putDeploymentInfo(Pair<UUID, UUID> paramPair, AuthToken paramAuthToken)
    throws MetaDataRepositoryException;
  
  public abstract void putServer(MetaInfo.Server paramServer, AuthToken paramAuthToken)
    throws MetaDataRepositoryException;
  
  public abstract void putMetaObject(MetaInfo.MetaObject paramMetaObject, AuthToken paramAuthToken)
    throws MetaDataRepositoryException;
  
  public abstract MetaInfo.MetaObject getMetaObjectByUUID(UUID paramUUID, AuthToken paramAuthToken)
    throws MetaDataRepositoryException;
  
  public abstract MetaInfo.MetaObject getMetaObjectByName(EntityType paramEntityType, String paramString1, String paramString2, Integer paramInteger, AuthToken paramAuthToken)
    throws MetaDataRepositoryException;
  
  public abstract Set getByEntityType(EntityType paramEntityType, AuthToken paramAuthToken)
    throws MetaDataRepositoryException;
  
  public abstract Set<MetaInfo.MetaObject> getByNameSpace(String paramString, AuthToken paramAuthToken)
    throws MetaDataRepositoryException;
  
  public abstract Set<?> getByEntityTypeInNameSpace(EntityType paramEntityType, String paramString, AuthToken paramAuthToken)
    throws MetaDataRepositoryException;
  
  public abstract Set<?> getByEntityTypeInApplication(EntityType paramEntityType, String paramString1, String paramString2, AuthToken paramAuthToken)
    throws MetaDataRepositoryException;
  
  public abstract MetaInfo.StatusInfo getStatusInfo(UUID paramUUID, AuthToken paramAuthToken)
    throws MetaDataRepositoryException;
  
  public abstract Collection<UUID> getServersForDeployment(UUID paramUUID, AuthToken paramAuthToken)
    throws MetaDataRepositoryException;
  
  public abstract MetaInfo.MetaObject getServer(String paramString, AuthToken paramAuthToken)
    throws MetaDataRepositoryException;
  
  public abstract Integer getMaxClassId();
  
  public abstract Position getAppCheckpointForFlow(UUID paramUUID, AuthToken paramAuthToken)
    throws MetaDataRepositoryException;
  
  public abstract MetaInfo.MetaObject revert(EntityType paramEntityType, String paramString1, String paramString2, AuthToken paramAuthToken)
    throws MetaDataRepositoryException;
  
  public abstract void removeMetaObjectByName(EntityType paramEntityType, String paramString1, String paramString2, Integer paramInteger, AuthToken paramAuthToken)
    throws MetaDataRepositoryException;
  
  public abstract void removeStatusInfo(UUID paramUUID, AuthToken paramAuthToken)
    throws MetaDataRepositoryException;
  
  public abstract void removeDeploymentInfo(Pair<UUID, UUID> paramPair, AuthToken paramAuthToken)
    throws MetaDataRepositoryException;
  
  public abstract void removeMetaObjectByUUID(UUID paramUUID, AuthToken paramAuthToken)
    throws MetaDataRepositoryException;
  
  public abstract void updateMetaObject(MetaInfo.MetaObject paramMetaObject, AuthToken paramAuthToken)
    throws MetaDataRepositoryException;
  
  public abstract boolean contains(MDConstants.typeOfRemove paramtypeOfRemove, UUID paramUUID, String paramString);
  
  public abstract boolean clear(boolean paramBoolean);
  
  public abstract String registerListerForDeploymentInfo(EntryListener<UUID, UUID> paramEntryListener);
  
  public abstract String registerListenerForShowStream(MessageListener<MetaInfo.ShowStream> paramMessageListener);
  
  public abstract String registerListenerForStatusInfo(EntryListener<UUID, MetaInfo.StatusInfo> paramEntryListener);
  
  public abstract String registerListenerForMetaObject(EntryListener<String, MetaInfo.MetaObject> paramEntryListener);
  
  public abstract String registerListenerForServer(EntryListener<UUID, MetaInfo.Server> paramEntryListener);
  
  public abstract void removeListerForDeploymentInfo(String paramString);
  
  public abstract void removeListenerForShowStream(String paramString);
  
  public abstract void removeListenerForStatusInfo(String paramString);
  
  public abstract void removeListenerForMetaObject(String paramString);
  
  public abstract void removeListenerForServer(String paramString);
  
  public abstract String exportMetadataAsJson();
  
  public abstract void importMetadataFromJson(String paramString, Boolean paramBoolean);
  
  public abstract void putAppCheckpointForFlow(Pair<UUID, Position> paramPair, AuthToken paramAuthToken)
    throws MetaDataRepositoryException;
  
  public abstract Map<?, ?> dumpMaps();
  
  public abstract boolean removeDGInfoFromServer(MetaInfo.MetaObject paramMetaObject, AuthToken paramAuthToken)
    throws MetaDataRepositoryException;
}

