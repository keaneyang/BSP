package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;

public class ImportDataStmt
  extends Stmt
{
  public final String what;
  public final String where;
  public final Boolean replace;
  
  public ImportDataStmt(String what, String where, Boolean replace)
  {
    this.what = what;
    this.where = where;
    this.replace = replace;
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileImportDataStmt(this);
  }
}
