package com.bloom.sourcefiltering;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.compiler.stmts.CreateSourceOrTargetStmt;

abstract interface Handler
{
  public abstract Object handle(Compiler paramCompiler, CreateSourceOrTargetStmt paramCreateSourceOrTargetStmt)
    throws MetaDataRepositoryException;
}

