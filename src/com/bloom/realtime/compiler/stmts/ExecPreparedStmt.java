package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.Property;
import com.bloom.runtime.compiler.Compiler;

import java.util.List;

public class ExecPreparedStmt
  extends Stmt
{
  public final String queryName;
  public final List<Property> params;
  
  public ExecPreparedStmt(String queryName, List<Property> params)
  {
    this.queryName = queryName;
    this.params = params;
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.execPreparedQuery(this);
  }
}
