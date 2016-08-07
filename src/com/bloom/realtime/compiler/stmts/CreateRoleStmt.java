package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.components.EntityType;

public class CreateRoleStmt
  extends CreateStmt
{
  public CreateRoleStmt(String name)
  {
    super(EntityType.ROLE, name, false);
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileCreateRoleStmt(this);
  }
}
