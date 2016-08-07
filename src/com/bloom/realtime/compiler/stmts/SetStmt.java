package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;

import java.io.Serializable;

public class SetStmt
  extends Stmt
{
  public final String paramname;
  public final Serializable paramvalue;
  
  public SetStmt(String paramname, Serializable paramvalue)
  {
    this.paramname = paramname;
    this.paramvalue = paramvalue;
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileSetStmt(this);
  }
}
