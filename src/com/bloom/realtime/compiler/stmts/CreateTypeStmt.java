package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.compiler.TypeDefOrName;
import com.bloom.runtime.components.EntityType;

public class CreateTypeStmt
  extends CreateStmt
{
  public final TypeDefOrName typeDef;
  
  public CreateTypeStmt(String type_name, Boolean doReplace, TypeDefOrName typeDef)
  {
    super(EntityType.TYPE, type_name, doReplace.booleanValue());
    this.typeDef = typeDef;
  }
  
  public String toString()
  {
    return stmtToString() + " (" + this.typeDef + ")";
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileCreateTypeStmt(this);
  }
}
