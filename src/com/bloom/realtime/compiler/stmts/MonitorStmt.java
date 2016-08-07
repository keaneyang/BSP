package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.compiler.Compiler;

import java.util.ArrayList;
import java.util.List;

public class MonitorStmt
  extends Stmt
{
  public final List<String> params;
  
  public MonitorStmt(List<String> params)
  {
    if (params == null) {
      this.params = new ArrayList();
    } else {
      this.params = params;
    }
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    c.compileMonitorStmt(this);
    return null;
  }
}
