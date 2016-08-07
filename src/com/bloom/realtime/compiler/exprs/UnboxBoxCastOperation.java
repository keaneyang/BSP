package com.bloom.runtime.compiler.exprs;

import com.bloom.runtime.compiler.CompilerUtils;

public class UnboxBoxCastOperation
  extends CastOperation
{
  public UnboxBoxCastOperation(ValueExpr arg, Class<?> targetType)
  {
    super(arg, targetType);
  }
  
  public String toString()
  {
    String targetType = getType().getCanonicalName();
    String unboxedSourceType = CompilerUtils.getUnboxingType(getExpr().getType()).getCanonicalName();
    
    String unboxedTargetType = CompilerUtils.getUnboxingType(getType()).getCanonicalName();
    
    return targetType + ".valueOf((" + unboxedTargetType + ")(" + getExpr() + ")." + unboxedSourceType + "Value())";
  }
}
