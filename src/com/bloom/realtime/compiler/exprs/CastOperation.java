package com.bloom.runtime.compiler.exprs;

import java.util.List;

import com.bloom.runtime.compiler.AST;
import com.bloom.runtime.compiler.visitors.ExpressionVisitor;

public class CastOperation
  extends Operation
{
  private final Class<?> targetType;
  
  public CastOperation(ValueExpr arg, Class<?> targetType)
  {
    super(ExprCmd.CAST, AST.NewList(arg));
    this.targetType = targetType;
    assert (targetType != null);
  }
  
  public String toString()
  {
    return exprToString() + " ((" + getType() + ")" + getExpr() + ")";
  }
  
  public <T> Expr visit(ExpressionVisitor<T> visitor, T params)
  {
    return visitor.visitCastOperation(this, params);
  }
  
  public ValueExpr getExpr()
  {
    return (ValueExpr)this.args.get(0);
  }
  
  public Class<?> getType()
  {
    return this.targetType;
  }
}
