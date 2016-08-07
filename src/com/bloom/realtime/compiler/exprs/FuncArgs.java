package com.bloom.runtime.compiler.exprs;

import java.util.List;

public class FuncArgs
{
  public final List<ValueExpr> args;
  public final int options;
  
  public FuncArgs(List<ValueExpr> args, int options)
  {
    this.args = args;
    this.options = options;
  }
}
