package com.bloom.runtime.compiler.exprs;

import java.util.List;

public class MethodCall
  extends FuncCall
{
  public MethodCall(String funcName, List<ValueExpr> args, int options)
  {
    super(funcName, args, options);
  }
  
  public ValueExpr getThis()
  {
    return (ValueExpr)this.args.get(0);
  }
  
  public List<ValueExpr> getFuncArgs()
  {
    return this.args.subList(1, this.args.size());
  }
  
  public boolean isConst()
  {
    if (getFuncArgs().isEmpty()) {
      return false;
    }
    return super.isConst();
  }
}
