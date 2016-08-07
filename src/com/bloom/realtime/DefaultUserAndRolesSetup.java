package com.bloom.runtime;

import com.bloom.exception.SecurityException;
import com.bloom.metaRepository.MDRepository;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Role;
import com.bloom.runtime.meta.MetaInfo.User;
import com.bloom.security.ObjectPermission;
import com.bloom.security.WASecurityManager;
import com.bloom.security.ObjectPermission.Action;
import com.bloom.security.ObjectPermission.ObjectType;
import com.bloom.uuid.AuthToken;

import java.io.Console;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.log4j.Logger;

public class DefaultUserAndRolesSetup
{
  private static Logger logger = Logger.getLogger(DefaultUserAndRolesSetup.class);
  private static MDRepository metadataRepository;
  private static WASecurityManager security_manager;
  private static AuthToken sessionID = WASecurityManager.TOKEN;
  
  public static void Setup(WASecurityManager security_manager)
    throws Exception
  {
    metadataRepository = MetadataRepository.getINSTANCE();
    security_manager = security_manager;
    createAdminUserAndRoleSetup();
  }
  
  private static void createAdminUserAndRoleSetup()
    throws Exception
  {
    if (security_manager.getUser("admin") == null)
    {
      String adminPassword = System.getProperty("com.bloom.config.adminPassword", System.getenv("WA_ADMIN_PASSWORD"));
      String password = null;
      boolean verified = false;
      if ((adminPassword != null) && (!adminPassword.isEmpty()))
      {
        if (logger.isInfoEnabled()) {
          logger.info("Using password sent through system properties : " + adminPassword);
        }
        password = adminPassword;
        verified = true;
      }
      else
      {
        printf("Required property \"Admin Password\" is undefined\n");
        password = ConsoleReader.readPassword("Enter Admin Password: ");
        int count = 0;
        while (count < 3)
        {
          String passToString2 = ConsoleReader.readPassword("Re-enter the password : ");
          if (password.equals(passToString2))
          {
            if (logger.isInfoEnabled()) {
              logger.info("Matched Password");
            }
            verified = true;
            break;
          }
          logger.error("Password did not match");
          verified = false;
          
          count++;
        }
      }
      if (verified)
      {
        createAdmin(password);
        createDefaultRoles();
        if (logger.isInfoEnabled()) {
          logger.info("done creating default roles - operator, appadmin, appdev, appuser");
        }
      }
      else
      {
        System.exit(1);
      }
    }
  }
  
  private static void createAdmin(String password)
    throws Exception
  {
    if (metadataRepository.getMetaObjectByName(EntityType.ROLE, "Global", "admin", null, sessionID) == null)
    {
      MetaInfo.Role newRole = new MetaInfo.Role();
      newRole.construct(MetaInfo.GlobalNamespace, "admin");
      newRole.grantPermission(new ObjectPermission("*"));
      metadataRepository.putMetaObject(newRole, sessionID);
    }
    if (security_manager.getUser("admin") == null)
    {
      MetaInfo.User newuser = null;
      
      MetaInfo.Role r = (MetaInfo.Role)metadataRepository.getMetaObjectByName(EntityType.ROLE, "Global", "admin", null, sessionID);
      List<String> lrole = new ArrayList();
      lrole.add(r.getRole());
      
      newuser = new MetaInfo.User();
      newuser.construct("admin", password);
      security_manager.addUser(newuser, sessionID);
      
      newuser = security_manager.getUser("admin");
      security_manager.grantUserRoles(newuser.getUserId(), lrole, sessionID);
      newuser = security_manager.getUser("admin");
    }
  }
  
  private static void createDefaultRoles()
    throws Exception
  {
    if (security_manager.getRole("Global:appadmin") == null)
    {
      MetaInfo.Role role = new MetaInfo.Role();
      role.construct(MetaInfo.GlobalNamespace, "appadmin");
      role.grantPermissions(getDefaultAppAdminPermissions());
      security_manager.addRole(role, sessionID);
    }
    if (security_manager.getRole("Global:appdev") == null)
    {
      MetaInfo.Role role = new MetaInfo.Role();
      role.construct(MetaInfo.GlobalNamespace, "appdev");
      role.grantPermissions(getDefaultDevPermissions());
      security_manager.addRole(role, sessionID);
    }
    if (security_manager.getRole("Global:appuser") == null)
    {
      MetaInfo.Role role = new MetaInfo.Role();
      role.construct(MetaInfo.GlobalNamespace, "appuser");
      role.grantPermissions(getDefaultEndUserPermissions());
      security_manager.addRole(role, sessionID);
    }
    if (security_manager.getRole("Global:systemuser") == null)
    {
      MetaInfo.Role role = new MetaInfo.Role();
      role.construct(MetaInfo.GlobalNamespace, "systemuser");
      role.grantPermissions(getDefaultSystemUserPermissions());
      security_manager.addRole(role, sessionID);
    }
    if (security_manager.getRole("Global:uiuser") == null)
    {
      MetaInfo.Role role = new MetaInfo.Role();
      role.construct(MetaInfo.GlobalNamespace, "uiuser");
      role.grantPermissions(getDefaultUIUserPermissions());
      security_manager.addRole(role, sessionID);
    }
  }
  
  private static List<ObjectPermission> getDefaultSystemUserPermissions()
    throws Exception
  {
    List<ObjectPermission> defaultUserPermissions = new ArrayList();
    
    Set<String> domains = new HashSet();
    Set<ObjectPermission.Action> actions = new HashSet();
    Set<ObjectPermission.ObjectType> objectTypes = new HashSet();
    Set<String> names = new HashSet();
    
    Set<String> domains1 = new HashSet();
    Set<ObjectPermission.Action> actions1 = new HashSet();
    Set<ObjectPermission.ObjectType> objectTypes1 = new HashSet();
    domains1.add("Global");
    actions1.add(ObjectPermission.Action.select);
    actions1.add(ObjectPermission.Action.read);
    objectTypes1.add(ObjectPermission.ObjectType.propertytemplate);
    objectTypes1.add(ObjectPermission.ObjectType.deploymentgroup);
    objectTypes1.add(ObjectPermission.ObjectType.type);
    
    ObjectPermission readPropTemplatesPerm = new ObjectPermission(domains1, actions1, objectTypes1, null);
    defaultUserPermissions.add(readPropTemplatesPerm);
    return defaultUserPermissions;
  }
  
  private static List<ObjectPermission> getDefaultUIUserPermissions()
    throws Exception
  {
    List<ObjectPermission> permissions = new ArrayList();
    ObjectPermission p1 = new ObjectPermission("*:*:dashboard_ui,apps_ui,sourcepreview_ui,monitor_ui:*");
    permissions.add(p1);
    return permissions;
  }
  
  private static List<ObjectPermission> getDefaultAppAdminPermissions()
    throws Exception
  {
    List<ObjectPermission> permissions = new ArrayList();
    ObjectPermission p1 = new ObjectPermission("Global:create:namespace:*");
    permissions.add(p1);
    ObjectPermission p2 = new ObjectPermission("Global:read,select:*:*");
    permissions.add(p2);
    return permissions;
  }
  
  private static List<ObjectPermission> getDefaultDevPermissions()
    throws SecurityException
  {
    List<ObjectPermission> permissions = new ArrayList();
    ObjectPermission p1 = new ObjectPermission("Global:namespace:*");
    permissions.add(p1);
    ObjectPermission p2 = new ObjectPermission("Global:read,select:*:*");
    permissions.add(p2);
    return permissions;
  }
  
  private static List<ObjectPermission> getDefaultEndUserPermissions()
    throws SecurityException
  {
    List<ObjectPermission> permissions = new ArrayList();
    ObjectPermission p2 = new ObjectPermission("Global:read,select:*:*");
    permissions.add(p2);
    return permissions;
  }
  
  private static void printf(String toPrint)
  {
    if (System.console() != null) {
      System.console().printf(toPrint, new Object[0]);
    } else {
      System.out.print(toPrint);
    }
    System.out.flush();
  }
  
  public static void main(String[] args) {}
}
