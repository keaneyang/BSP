package com.bloom.runtime.compiler.exprs;

public class CastToStringOperation
  extends CastOperation
{
  public CastToStringOperation(ValueExpr arg)
  {
    super(arg, String.class);
  }
  
  public String toString()
  {
    return "(" + getExpr() + ").toString()";
  }
}
