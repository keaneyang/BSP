package com.bloom.runtime.components;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.metaRepository.PermissionUtility;
import com.bloom.runtime.meta.MetaInfo.PropertyVariable;
import com.bloom.security.WASecurityManager;
import com.bloom.security.ObjectPermission.Action;
import com.bloom.uuid.AuthToken;

import java.util.Map;
import java.util.Map.Entry;
import org.apache.log4j.Logger;

public abstract class PropertyVariablePermissionChecker
{
  private static final Logger logger = Logger.getLogger(PropertyVariablePermissionChecker.class);
  
  public static boolean propertyVariableAccessChecker(AuthToken token, String namespace, Map<String, Object>... properties)
    throws MetaDataRepositoryException
  {
    for (Map<String, Object> property : properties) {
      for (Map.Entry entry : property.entrySet()) {
        if (((entry.getValue() instanceof String)) && (entry.getValue().toString().trim().startsWith("$")))
        {
          String propEnvKey = entry.getValue().toString().trim().substring(1);
          if ((propEnvKey != null) && (!propEnvKey.trim().isEmpty()) && 
            (propEnvKey.contains(".")))
          {
            String[] splitKeys = propEnvKey.split("\\.");
            propEnvKey = splitKeys[1];
            namespace = splitKeys[0];
          }
          MetaInfo.PropertyVariable obj = (MetaInfo.PropertyVariable)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.PROPERTYVARIABLE, namespace, propEnvKey, null, WASecurityManager.TOKEN);
          if (obj == null) {
            return true;
          }
          try
          {
            boolean check = PermissionUtility.checkPermission(obj, ObjectPermission.Action.deploy, token, true);
            if (!check) {
              return false;
            }
            return true;
          }
          catch (MetaDataRepositoryException e)
          {
            logger.error("Security Exception while trying to access prop variable object" + e.getMessage());
          }
        }
      }
    }
    return true;
  }
}
