package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;

public class ExportStreamSchemaStmt
  extends Stmt
{
  public final String streamName;
  public final String optPath;
  public final String optFileName;
  
  public ExportStreamSchemaStmt(String streamname, String optPath, String optFileName)
  {
    this.streamName = streamname;
    this.optPath = optPath;
    this.optFileName = optFileName;
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.exportStreamSchema(this);
  }
}
