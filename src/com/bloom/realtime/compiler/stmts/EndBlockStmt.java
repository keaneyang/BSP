package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.components.EntityType;

public class EndBlockStmt
  extends Stmt
{
  public final String name;
  public final EntityType type;
  
  public EndBlockStmt(String name, EntityType type)
  {
    this.name = name;
    this.type = type;
  }
  
  public String toString()
  {
    return "END " + this.type + " " + this.name;
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileEndStmt(this);
  }
}
