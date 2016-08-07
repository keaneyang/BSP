package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;

public class EmptyStmt
  extends Stmt
{
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return null;
  }
}