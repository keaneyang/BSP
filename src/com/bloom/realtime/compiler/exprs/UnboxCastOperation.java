package com.bloom.runtime.compiler.exprs;

public class UnboxCastOperation
  extends CastOperation
{
  public UnboxCastOperation(ValueExpr arg, Class<?> targetType)
  {
    super(arg, targetType);
  }
  
  public String toString()
  {
    Class<?> t = getType();
    return "(" + getExpr() + ")." + t.getSimpleName() + "Value()";
  }
}
