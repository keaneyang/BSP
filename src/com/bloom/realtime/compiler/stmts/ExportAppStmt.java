package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;

public class ExportAppStmt
  extends Stmt
{
  public final String appName;
  public final String filepath;
  public final String format;
  
  public ExportAppStmt(String appName, String destJar, String format)
  {
    this.appName = appName;
    this.filepath = destJar;
    this.format = format;
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileExportTypesStmt(this);
  }
}
