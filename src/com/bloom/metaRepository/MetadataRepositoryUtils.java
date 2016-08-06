package com.bloom.metaRepository;

import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Flow;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.security.WASecurityManager;
import com.bloom.uuid.UUID;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Logger;

public class MetadataRepositoryUtils
{
  private static Logger logger = Logger.getLogger(MetadataRepositoryUtils.class);
  
  public static MetaInfo.Flow getAppMetaObjectBelongsTo(MetaInfo.MetaObject currentMetaObj)
  {
    if (currentMetaObj == null) {
      return null;
    }
    if (currentMetaObj.getType().equals(EntityType.APPLICATION)) {
      return (MetaInfo.Flow)currentMetaObj;
    }
    Set<UUID> set = currentMetaObj.getReverseIndexObjectDependencies();
    if ((set != null) || (set.size() == 0))
    {
      Iterator<UUID> iter = set.iterator();
      while (iter.hasNext())
      {
        UUID parentObjId = (UUID)iter.next();
        try
        {
          MetaInfo.MetaObject parentObj = MetadataRepository.getINSTANCE().getMetaObjectByUUID(parentObjId, WASecurityManager.TOKEN);
          if ((parentObj.type == EntityType.APPLICATION) && 
            (isAppContainsThisStream((MetaInfo.Flow)parentObj, currentMetaObj)))
          {
            if (logger.isInfoEnabled()) {
              logger.info(currentMetaObj.name + " " + currentMetaObj.getType().name() + " belongs to " + parentObj.name);
            }
            return (MetaInfo.Flow)parentObj;
          }
          if (parentObj.type == EntityType.FLOW)
          {
            MetaInfo.Flow result = getAppMetaObjectBelongsTo(parentObj);
            if (result != null) {
              return result;
            }
          }
        }
        catch (MetaDataRepositoryException e)
        {
          logger.error("error getting application metainfo object", e);
        }
      }
    }
    return null;
  }
  
  public static boolean isAppContainsThisStream(MetaInfo.Flow app, MetaInfo.MetaObject metaObj)
  {
    if ((app == null) || (metaObj == null)) {
      return false;
    }
    Set<UUID> subSet = (Set)app.objects.get(metaObj.getType());
    if ((subSet != null) && (subSet.contains(metaObj.uuid))) {
      return true;
    }
    Set<UUID> flowSet = app.getObjects(EntityType.FLOW);
    if (flowSet != null)
    {
      Iterator<UUID> iter = flowSet.iterator();
      while (iter.hasNext())
      {
        UUID flowId = (UUID)iter.next();
        MetaInfo.Flow flowObject = null;
        try
        {
          flowObject = (MetaInfo.Flow)MetadataRepository.getINSTANCE().getMetaObjectByUUID(flowId, WASecurityManager.TOKEN);
        }
        catch (MetaDataRepositoryException e)
        {
          logger.error("error getting application metainfo object", e);
        }
        if (isAppContainsThisStream(flowObject, metaObj)) {
          return true;
        }
      }
    }
    return false;
  }
}
