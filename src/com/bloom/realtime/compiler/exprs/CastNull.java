package com.bloom.runtime.compiler.exprs;

public class CastNull
  extends CastOperation
{
  public CastNull(ValueExpr arg, Class<?> targetType)
  {
    super(arg, targetType);
  }
  
  public String toString()
  {
    Class<?> t = getType();
    return "(" + t.getName() + ")null";
  }
}
