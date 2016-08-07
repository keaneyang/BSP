package com.bloom.runtime.compiler.stmts;

import java.util.List;

import com.bloom.security.ObjectPermission.Action;
import com.bloom.security.ObjectPermission.ObjectType;

public abstract interface SecurityStmt
{
  public abstract String getName();
  
  public abstract List<ObjectPermission.Action> getListOfPrivilege();
  
  public abstract List<ObjectPermission.ObjectType> getObjectType();
}
