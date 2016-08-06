package com.bloom.metaRepository;

import com.bloom.exception.SecurityException;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfoStatus;
import com.bloom.runtime.meta.MetaInfo.Flow;
import com.bloom.runtime.meta.MetaInfo.MetaObject;
import com.bloom.runtime.meta.MetaInfo.Target;
import com.bloom.runtime.utils.NamePolicy;
import com.bloom.security.ObjectPermission;
import com.bloom.security.WASecurityManager;
import com.bloom.security.ObjectPermission.Action;
import com.bloom.security.ObjectPermission.ObjectType;
import com.bloom.security.ObjectPermission.PermissionType;
import com.bloom.uuid.AuthToken;

import org.apache.log4j.Logger;

public class PermissionUtility
{
  private static final String colon = ":";
  private static Logger logger = Logger.getLogger(MDCache.class);
  
  private static ObjectPermission.ObjectType getPermissionObjectType(EntityType eType)
  {
    switch (eType)
    {
    case UNKNOWN: 
      return ObjectPermission.ObjectType.unknown;
    case APPLICATION: 
      return ObjectPermission.ObjectType.application;
    case FLOW: 
      return ObjectPermission.ObjectType.flow;
    case STREAM: 
      return ObjectPermission.ObjectType.stream;
    case WINDOW: 
      return ObjectPermission.ObjectType.window;
    case TYPE: 
      return ObjectPermission.ObjectType.type;
    case CQ: 
      return ObjectPermission.ObjectType.cq;
    case QUERY: 
      return ObjectPermission.ObjectType.query;
    case SOURCE: 
      return ObjectPermission.ObjectType.source;
    case TARGET: 
      return ObjectPermission.ObjectType.target;
    case PROPERTYSET: 
      return ObjectPermission.ObjectType.propertyset;
    case PROPERTYVARIABLE: 
      return ObjectPermission.ObjectType.propertyvariable;
    case WACTIONSTORE: 
      return ObjectPermission.ObjectType.wactionstore;
    case PROPERTYTEMPLATE: 
      return ObjectPermission.ObjectType.propertytemplate;
    case CACHE: 
      return ObjectPermission.ObjectType.cache;
    case ALERTSUBSCRIBER: 
      return ObjectPermission.ObjectType.alertsubscriber;
    case SERVER: 
      return ObjectPermission.ObjectType.server;
    case USER: 
      return ObjectPermission.ObjectType.user;
    case ROLE: 
      return ObjectPermission.ObjectType.role;
    case INITIALIZER: 
      return ObjectPermission.ObjectType.initializer;
    case DG: 
      return ObjectPermission.ObjectType.deploymentgroup;
    case VISUALIZATION: 
      return ObjectPermission.ObjectType.visualization;
    case NAMESPACE: 
      return ObjectPermission.ObjectType.namespace;
    case DASHBOARD: 
      return ObjectPermission.ObjectType.dashboard;
    case PAGE: 
      return ObjectPermission.ObjectType.page;
    case QUERYVISUALIZATION: 
      return ObjectPermission.ObjectType.queryvisualization;
    case WI: 
      break;
    }
    return null;
  }
  
  public static ObjectPermission getCreateOrUpdatePermission(MetaInfo.MetaObject mObject, MDCache cache)
    throws SecurityException
  {
    String appName = mObject.nsName;
    if (logger.isDebugEnabled()) {
      logger.debug("Object key : " + NamePolicy.makeKey(new StringBuilder().append(mObject.nsName).append(":").append(mObject.getType()).append(":").append(mObject.name).toString()));
    }
    ObjectPermission permission;
    if (cache.contains(MDConstants.typeOfRemove.BY_NAME, null, NamePolicy.makeKey(mObject.nsName + ":" + mObject.getType() + ":" + mObject.name)))
    {
      permission = new ObjectPermission(appName, ObjectPermission.Action.update, getPermissionObjectType(mObject.type), mObject.name);
    }
    else
    {
      if (mObject.type == EntityType.NAMESPACE) {
        permission = new ObjectPermission("Global", ObjectPermission.Action.create, ObjectPermission.ObjectType.namespace, mObject.name);
      } else {
        permission = new ObjectPermission(appName, ObjectPermission.Action.create, getPermissionObjectType(mObject.type), mObject.name);
      }
    }
    return permission;
  }
  
  public static boolean checkReadPermission(MetaInfo.MetaObject mObject, AuthToken token, WASecurityManager sManager)
    throws MetaDataRepositoryException, SecurityException
  {
    MDConstants.checkNullParams("Can't pass NULL parameters while checking for READ permission on a MetaObject, \nMetaObject: " + mObject.getFullName() + "\nToken: " + token + "\nSecurity Manager: " + (sManager == null ? "Null" : "Present"), new Object[] { mObject, token, sManager });
    
    ObjectPermission permission = new ObjectPermission(mObject.nsName, ObjectPermission.Action.read, getPermissionObjectType(mObject.type), mObject.name);
    
    return sManager.isAllowedAndNotDisallowed(token, permission);
  }
  
  public static boolean checkPermission(MetaInfo.MetaObject obj, ObjectPermission.Action action, AuthToken token, boolean shouldThrow)
    throws SecurityException, MetaDataRepositoryException
  {
    MDConstants.checkNullParams("Can't pass NULL parameters while checking for permission on a MetaObject, \nMetaObject: " + obj.getFullName() + "\nToken: " + token, new Object[] { obj, token, action });
    if (((obj.getMetaInfoStatus().isAnonymous()) || (obj.getMetaInfoStatus().isAdhoc())) && 
      (action.equals(ObjectPermission.Action.create))) {
      return true;
    }
    WASecurityManager sm = WASecurityManager.get();
    if (sm == null) {
      return true;
    }
    ObjectPermission.ObjectType otype = getPermissionObjectType(obj.type);
    if ((obj.type == EntityType.TARGET) && (((MetaInfo.Target)obj).isSubscription())) {
      otype = ObjectPermission.ObjectType.subscription;
    }
    ObjectPermission permission = new ObjectPermission(obj.nsName, action, otype, obj.name, ObjectPermission.PermissionType.allow);
    if (!sm.isDisallowed(token, permission))
    {
      if (sm.isAllowed(token, permission)) {
        return true;
      }
      MetaInfo.Flow app = obj.getCurrentApp();
      if (app != null)
      {
        ObjectPermission appPermission = new ObjectPermission(app.nsName, action, getPermissionObjectType(app.type), app.name, ObjectPermission.PermissionType.allow);
        if (sm.isAllowedAndNotDisallowed(token, appPermission)) {
          return true;
        }
      }
    }
    if (shouldThrow) {
      throw new SecurityException("No permission to " + action + " " + obj.type.toString().toLowerCase() + " " + obj.nsName + "." + obj.name);
    }
    return false;
  }
}
