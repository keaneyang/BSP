package com.bloom.runtime.compiler.exprs;

import java.util.List;

import com.bloom.runtime.compiler.CompilerUtils;
import com.bloom.runtime.compiler.visitors.ExpressionVisitor;

public class ArrayInitializer
  extends Operation
{
  private final Class<?> arrayType;
  
  public ArrayInitializer(Class<?> cls, List<ValueExpr> arrayValues)
  {
    super(ExprCmd.ARRAYINIT, arrayValues);
    this.arrayType = CompilerUtils.toArrayType(cls);
  }
  
  public String toString()
  {
    return exprToString() + this.arrayType + "[] {" + argsToString() + "}";
  }
  
  public Class<?> getType()
  {
    return this.arrayType;
  }
  
  public <T> Expr visit(ExpressionVisitor<T> visitor, T params)
  {
    return visitor.visitArrayInitializer(this, params);
  }
}
