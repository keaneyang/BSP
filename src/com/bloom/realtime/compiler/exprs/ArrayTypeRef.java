package com.bloom.runtime.compiler.exprs;

import com.bloom.exception.CompilationException;
import com.bloom.runtime.compiler.TypeName;
import com.bloom.runtime.compiler.visitors.ExpressionVisitor;

public class ArrayTypeRef
  extends ValueExpr
{
  public final TypeName typeName;
  
  public ArrayTypeRef(TypeName typeName)
  {
    super(ExprCmd.ARRAYTYPE);
    this.typeName = typeName;
  }
  
  public <T> Expr visit(ExpressionVisitor<T> visitor, T params)
  {
    return visitor.visitArrayTypeRef(this, params);
  }
  
  public Expr resolve()
  {
    throw new CompilationException("expected expression, got type" + this.typeName);
  }
  
  public String toString()
  {
    return exprToString() + this.typeName;
  }
  
  public Class<?> getType()
  {
    if (!$assertionsDisabled) {
      throw new AssertionError();
    }
    return null;
  }
  
  public boolean isConst()
  {
    return true;
  }
}
