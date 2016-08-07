package com.bloom.runtime.compiler.exprs;

public class BoxCastOperation
  extends CastOperation
{
  public BoxCastOperation(ValueExpr arg, Class<?> targetType)
  {
    super(arg, targetType);
  }
  
  public String toString()
  {
    Class<?> t = getType();
    return t.getName() + ".valueOf(" + getExpr() + ")";
  }
}
