package com.bloom.runtime.compiler.exprs;

import java.util.List;

import com.bloom.runtime.compiler.visitors.ExpressionVisitor;

public class NumericOperation
  extends Operation
{
  private Class<?> resultType;
  
  public NumericOperation(ExprCmd op, List<ValueExpr> args)
  {
    super(op, args);
  }
  
  public <T> Expr visit(ExpressionVisitor<T> visitor, T params)
  {
    return visitor.visitNumericOperation(this, params);
  }
  
  public Class<?> getType()
  {
    assert (this.resultType != null);
    return this.resultType;
  }
  
  public void setType(Class<?> resType)
  {
    assert (resType != null);
    this.resultType = resType;
  }
}
