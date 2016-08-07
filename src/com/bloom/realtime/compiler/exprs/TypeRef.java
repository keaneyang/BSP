package com.bloom.runtime.compiler.exprs;

import com.bloom.exception.CompilationException;
import com.bloom.runtime.compiler.visitors.ExpressionVisitor;

public class TypeRef
  extends ValueExpr
{
  public final Class<?> klass;
  
  public TypeRef(Class<?> klass)
  {
    super(ExprCmd.TYPE);
    this.klass = klass;
  }
  
  public <T> Expr visit(ExpressionVisitor<T> visitor, T params)
  {
    return visitor.visitTypeRef(this, params);
  }
  
  public boolean isTypeRef()
  {
    return true;
  }
  
  public Expr resolve()
  {
    throw new CompilationException("expected expression, got type " + getType().getName());
  }
  
  public String toString()
  {
    return exprToString() + getType().getName();
  }
  
  public Class<?> getType()
  {
    return this.klass;
  }
  
  public boolean isConst()
  {
    return true;
  }
}
