package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;

public class CreateDashboardStatement
  extends Stmt
{
  private final String fileName;
  private final Boolean doReplace;
  
  public CreateDashboardStatement(Boolean doReplace, String fileName)
  {
    this.fileName = fileName;
    this.doReplace = doReplace;
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileCreateDashboardStatement(this);
  }
  
  public String getFileName()
  {
    return this.fileName;
  }
  
  public Boolean getDoReplace()
  {
    return this.doReplace;
  }
}
