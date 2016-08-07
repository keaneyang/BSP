package com.bloom.runtime.compiler.exprs;

import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.compiler.CompilerUtils;

public class CastExprFactory
{
  public static ValueExpr cast(Compiler compiler, ValueExpr e, Class<?> targetType)
  {
    Class<?> sourceType = e.getType();
    try
    {
      return castImpl(e, sourceType, targetType);
    }
    catch (ClassCastException ex)
    {
      compiler.error("cannot cast " + sourceType.getCanonicalName() + " into " + targetType.getCanonicalName(), e);
    }
    return e;
  }
  
  private static ValueExpr cast(ValueExpr e, Class<?> targetType)
  {
    Class<?> sourceType = e.getType();
    return castImpl(e, sourceType, targetType);
  }
  
  private static void castAssert(boolean predicate)
  {
    if (!predicate) {
      throw new ClassCastException();
    }
  }
  
  private static ValueExpr setOriginalExpr(ValueExpr e, Expr original)
  {
    e.setOriginalExpr(original);
    return e;
  }
  
  private static ValueExpr castImpl(ValueExpr e, Class<?> sourceType, Class<?> targetType)
  {
    castAssert(CompilerUtils.isCastable(targetType, sourceType));
    if (CompilerUtils.isNull(sourceType)) {
      return setOriginalExpr(new CastNull(e, targetType), e);
    }
    if (CompilerUtils.isParam(sourceType))
    {
      if (targetType.isPrimitive()) {
        return cast(cast(e, CompilerUtils.getBoxingType(targetType)), targetType);
      }
      ParamRef p = (ParamRef)e;
      p.setExpectedType(targetType);
      return setOriginalExpr(new CastParam(e, targetType), e);
    }
    if (targetType.isAssignableFrom(sourceType))
    {
      if (targetType.equals(sourceType)) {
        return e;
      }
      return setOriginalExpr(new CastOperation(e, targetType), e);
    }
    if (sourceType.isAssignableFrom(targetType)) {
      return setOriginalExpr(new CastOperation(e, targetType), e);
    }
    if ((sourceType.isPrimitive()) && (targetType.isPrimitive()))
    {
      castAssert(CompilerUtils.compatiblePrimitive(sourceType, targetType));
      
      return setOriginalExpr(new CastOperation(e, targetType), e);
    }
    Class<?> unboxedSource = CompilerUtils.getUnboxingType(sourceType);
    Class<?> unboxedTarget = CompilerUtils.getUnboxingType(targetType);
    if ((unboxedSource != null) && (unboxedTarget != null))
    {
      castAssert(CompilerUtils.compatiblePrimitive(unboxedSource, unboxedTarget));
      
      return setOriginalExpr(new UnboxBoxCastOperation(e, targetType), e);
    }
    if (sourceType.isPrimitive())
    {
      if (unboxedTarget != null)
      {
        castAssert(CompilerUtils.compatiblePrimitive(sourceType, unboxedTarget));
        
        e = setOriginalExpr(castImpl(e, sourceType, unboxedTarget), e);
        
        return setOriginalExpr(new BoxCastOperation(e, targetType), e);
      }
      Class<?> boxedSource = CompilerUtils.getBoxingType(sourceType);
      castAssert(CompilerUtils.compatible(boxedSource, targetType));
      
      e = setOriginalExpr(new BoxCastOperation(e, boxedSource), e);
      
      return setOriginalExpr(castImpl(e, boxedSource, targetType), e);
    }
    if (targetType.isPrimitive())
    {
      if (unboxedSource != null)
      {
        castAssert(CompilerUtils.compatiblePrimitive(unboxedSource, targetType));
        
        e = setOriginalExpr(new UnboxCastOperation(e, unboxedSource), e);
        
        return setOriginalExpr(castImpl(e, unboxedSource, targetType), e);
      }
      Class<?> boxedTarget = CompilerUtils.getBoxingType(targetType);
      castAssert(CompilerUtils.compatible(boxedTarget, sourceType));
      
      e = setOriginalExpr(castImpl(e, sourceType, boxedTarget), e);
      
      return setOriginalExpr(new UnboxCastOperation(e, targetType), e);
    }
    castAssert(false);
    return e;
  }
}
