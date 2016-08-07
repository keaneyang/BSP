package com.bloom.runtime.compiler.stmts;

import com.bloom.exception.FatalException;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;

public class LoadFileStmt
  extends Stmt
{
  String fileName = "";
  
  public LoadFileStmt(String fname)
  {
    this.fileName = fname;
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    try
    {
      return c.compileLoadFileStmt(this);
    }
    catch (Exception e)
    {
      throw new FatalException(e);
    }
  }
  
  public String getFileName()
  {
    return this.fileName;
  }
}
