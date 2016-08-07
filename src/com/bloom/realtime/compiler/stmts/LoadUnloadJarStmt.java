package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;

public class LoadUnloadJarStmt
  extends Stmt
{
  public final String pathToJar;
  public final boolean doLoad;
  
  public LoadUnloadJarStmt(String pathToJar, boolean doLoad)
  {
    this.pathToJar = pathToJar;
    this.doLoad = doLoad;
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileLoadOrUnloadJar(this);
  }
}
