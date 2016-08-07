package com.bloom.runtime.compiler.exprs;

import java.util.List;

import com.bloom.runtime.compiler.visitors.ExpressionVisitor;

public class StringConcatenation
  extends Operation
{
  public StringConcatenation(List<ValueExpr> args)
  {
    super(ExprCmd.CONCAT, args);
  }
  
  public Class<?> getType()
  {
    return String.class;
  }
  
  public <T> Expr visit(ExpressionVisitor<T> visitor, T params)
  {
    return visitor.visitStringConcatenation(this, params);
  }
}
