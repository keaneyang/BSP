package com.bloom.sourcefiltering;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.compiler.stmts.CreateSourceOrTargetStmt;

public class SourceSideFilteringManager
  implements Manager
{
  public Object receive(Handler handler, Compiler compiler, CreateSourceOrTargetStmt createSourceWithImplicitCQStmt)
    throws MetaDataRepositoryException
  {
    return handler.handle(compiler, createSourceWithImplicitCQStmt);
  }
}
