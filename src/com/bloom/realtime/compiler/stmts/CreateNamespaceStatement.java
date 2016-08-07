package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.components.EntityType;

public class CreateNamespaceStatement
  extends CreateStmt
{
  public CreateNamespaceStatement(String name, Boolean doReplace)
  {
    super(EntityType.NAMESPACE, name, doReplace.booleanValue());
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileCreateNamespaceStatement(this);
  }
}
