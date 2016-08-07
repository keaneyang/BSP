package com.bloom.runtime.compiler.exprs;

import com.bloom.runtime.compiler.TypeName;
import com.bloom.runtime.compiler.visitors.ExpressionVisitor;

public class ClassRef
  extends ValueExpr
{
  public final TypeName typeName;
  
  public ClassRef(TypeName typeName)
  {
    super(ExprCmd.CLASS);
    this.typeName = typeName;
  }
  
  public <T> Expr visit(ExpressionVisitor<T> visitor, T params)
  {
    return visitor.visitClassRef(this, params);
  }
  
  public Class<?> getType()
  {
    return Class.class;
  }
  
  public boolean isConst()
  {
    return true;
  }
}
