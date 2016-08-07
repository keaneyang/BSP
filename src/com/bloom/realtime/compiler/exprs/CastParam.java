package com.bloom.runtime.compiler.exprs;

public class CastParam
  extends CastOperation
{
  public CastParam(ValueExpr arg, Class<?> targetType)
  {
    super(arg, targetType);
  }
}
