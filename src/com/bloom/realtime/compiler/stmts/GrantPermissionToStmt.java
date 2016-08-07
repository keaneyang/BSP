package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.components.EntityType;
import com.bloom.security.ObjectPermission;
import com.bloom.security.ObjectPermission.Action;
import com.bloom.security.ObjectPermission.ObjectType;

import java.util.List;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

public class GrantPermissionToStmt
  extends Stmt
  implements SecurityStmt
{
  public final EntityType towhat;
  List<ObjectPermission.Action> listOfPrivilege;
  List<ObjectPermission.ObjectType> objectType;
  public String objectName;
  public String name;
  
  public GrantPermissionToStmt(@NotNull List<ObjectPermission.Action> listOfPrivilege, @Nullable List<ObjectPermission.ObjectType> objectType, @NotNull String name, @Nullable String userorpermissionname, EntityType what)
  {
    this.towhat = what;
    this.listOfPrivilege = listOfPrivilege;
    this.objectType = objectType;
    this.objectName = name;
    this.name = userorpermissionname;
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.GrantPermissionToStmt(this);
  }
  
  public String getName()
  {
    return this.objectName;
  }
  
  public List<ObjectPermission.Action> getListOfPrivilege()
  {
    return this.listOfPrivilege;
  }
  
  public List<ObjectPermission.ObjectType> getObjectType()
  {
    return this.objectType;
  }
}
