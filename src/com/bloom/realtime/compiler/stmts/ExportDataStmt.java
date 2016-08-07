package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;

public class ExportDataStmt
  extends Stmt
{
  public final String what;
  public final String where;
  
  public ExportDataStmt(String what, String where)
  {
    this.what = what;
    this.where = where;
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileExportDataStmt(this);
  }
}
