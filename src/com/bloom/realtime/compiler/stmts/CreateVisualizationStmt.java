package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.components.EntityType;

public class CreateVisualizationStmt
  extends CreateStmt
{
  public String fileName;
  
  public CreateVisualizationStmt(String objectName, String filename)
  {
    super(EntityType.VISUALIZATION, objectName, false);
    this.fileName = filename;
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileCreateVisualizationStmt(this);
  }
}
