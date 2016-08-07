package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.components.EntityType;

import java.util.List;

public class RevokeRoleFromStmt
  extends Stmt
{
  public final EntityType towhat;
  public final List<String> rolename;
  public final String name;
  
  public RevokeRoleFromStmt(List<String> rolename, String name, EntityType what)
  {
    this.towhat = what;
    this.rolename = rolename;
    this.name = name;
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.RevokeRoleFromStmt(this);
  }
}
