package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.Context;
import com.bloom.runtime.compiler.Compiler;

public class DumpStmt
  extends Stmt
{
  String cqname;
  int dumpmode;
  int action;
  public static final int DUMP_INPUT = 1;
  public static final int DUMP_OUTPUT = 2;
  public static final int DUMP_INPUTOUTPUT = 3;
  public static final int DUMP_CODE = 4;
  public static final int ACTION_ON = 1;
  public static final int ACTION_OFF = 2;
  
  public DumpStmt(String cqname, int mode, int action)
  {
    this.cqname = cqname;
    this.dumpmode = mode;
    this.action = action;
  }
  
  public String getCqname()
  {
    return this.cqname;
  }
  
  public int getDumpmode()
  {
    return this.dumpmode;
  }
  
  public int getAction()
  {
    return this.action;
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    c.compileDumpStmt(this.cqname, this.dumpmode, this.action, c.getContext().getAuthToken());
    return this;
  }
}
