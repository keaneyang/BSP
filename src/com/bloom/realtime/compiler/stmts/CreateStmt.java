package com.bloom.runtime.compiler.stmts;

import com.bloom.exception.CompilationException;
import com.bloom.runtime.TraceOptions;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.components.EntityType;

public abstract class CreateStmt
  extends Stmt
{
  public final EntityType what;
  public final String name;
  public final boolean doReplace;
  
  public CreateStmt(EntityType what, String name, boolean doReplace)
  {
    this.what = what;
    this.name = name;
    this.doReplace = doReplace;
  }
  
  public String stmtToString()
  {
    return "CREATE " + this.what + (this.doReplace ? " OR REPLACE" : "") + " " + this.name;
  }
  
  public String toString()
  {
    return stmtToString();
  }
  
  public void checkValidity(String exceptionMessage, Object... args)
    throws CompilationException
  {
    if (args == null) {
      return;
    }
    for (Object arg : args) {
      if (arg == null) {
        throw new CompilationException(exceptionMessage);
      }
    }
  }
  
  public boolean isWithDebugEnabled()
  {
    TraceOptions traceOptions = Compiler.buildTraceOptions(this);
    return traceOptions.traceFlags == 64;
  }
}
