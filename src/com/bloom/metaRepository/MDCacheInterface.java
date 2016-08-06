package com.bloom.metaRepository;

import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.Server;
import com.bloom.runtime.meta.MetaInfo.ShowStream;
import com.bloom.runtime.meta.MetaInfo.StatusInfo;
import com.bloom.uuid.UUID;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.MessageListener;

import java.util.EventListener;

public abstract interface MDCacheInterface
{
  public abstract boolean put(Object paramObject);
  
  public abstract Object get(EntityType paramEntityType, UUID paramUUID, String paramString1, String paramString2, Integer paramInteger, MDConstants.typeOfGet paramtypeOfGet);
  
  public abstract Object remove(EntityType paramEntityType, Object paramObject, String paramString1, String paramString2, Integer paramInteger, MDConstants.typeOfRemove paramtypeOfRemove);
  
  public abstract boolean contains(MDConstants.typeOfRemove paramtypeOfRemove, UUID paramUUID, String paramString);
  
  public abstract boolean clear();
  
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
  
  public abstract String registerCacheEntryListener(EventListener paramEventListener, MDConstants.mdcListener parammdcListener);
}

