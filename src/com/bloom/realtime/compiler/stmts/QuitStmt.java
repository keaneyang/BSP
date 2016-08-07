package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.HazelcastSingleton;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;

public class QuitStmt
  extends Stmt
{
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    if (HazelcastSingleton.isClientMember()) {
      com.bloom.tungsten.Tungsten.quit = true;
    }
    return null;
  }
}
