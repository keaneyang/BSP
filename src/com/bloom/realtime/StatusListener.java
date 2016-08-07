package com.bloom.runtime;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.StatusInfo;
import com.bloom.runtime.monitor.MonitorCollector;
import com.bloom.security.WASecurityManager;
import com.bloom.uuid.UUID;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.MapEvent;

import org.apache.log4j.Logger;

public class StatusListener
  implements EntryListener<UUID, MetaInfo.StatusInfo>
{
  private static Logger logger = Logger.getLogger(StatusListener.class);
  
  public void entryAdded(EntryEvent<UUID, MetaInfo.StatusInfo> entryEvent)
  {
    if (((MetaInfo.StatusInfo)entryEvent.getValue()).getType().equals(EntityType.APPLICATION)) {
      try
      {
        MetaInfo.MetaObject mo = MetadataRepository.getINSTANCE().getMetaObjectByUUID((UUID)entryEvent.getKey(), WASecurityManager.TOKEN);
        if ((!mo.getName().contains("MonitoringSourceApp")) && (!mo.getName().contains("MonitoringProcessApp"))) {
          MonitorCollector.reportStateChange((UUID)entryEvent.getKey(), ((MetaInfo.StatusInfo)entryEvent.getValue()).serializeToString());
        }
      }
      catch (MetaDataRepositoryException e)
      {
        logger.warn(e.getMessage(), e);
      }
    }
  }
  
  public void entryRemoved(EntryEvent<UUID, MetaInfo.StatusInfo> entryEvent) {}
  
  public void entryUpdated(EntryEvent<UUID, MetaInfo.StatusInfo> entryEvent)
  {
    if (((MetaInfo.StatusInfo)entryEvent.getValue()).getType().equals(EntityType.APPLICATION)) {
      try
      {
        MetaInfo.MetaObject mo = MetadataRepository.getINSTANCE().getMetaObjectByUUID((UUID)entryEvent.getKey(), WASecurityManager.TOKEN);
        if ((!mo.getName().contains("MonitoringSourceApp")) && (!mo.getName().contains("MonitoringProcessApp"))) {
          MonitorCollector.reportStateChange((UUID)entryEvent.getKey(), ((MetaInfo.StatusInfo)entryEvent.getValue()).serializeToString());
        }
      }
      catch (MetaDataRepositoryException e)
      {
        logger.warn(e.getMessage(), e);
      }
    }
  }
  
  public void entryEvicted(EntryEvent<UUID, MetaInfo.StatusInfo> entryEvent) {}
  
  public void mapEvicted(MapEvent mapEvent) {}
  
  public void mapCleared(MapEvent mapEvent) {}
}
