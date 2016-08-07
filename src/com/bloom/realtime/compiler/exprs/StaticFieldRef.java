package com.bloom.runtime.compiler.exprs;

import java.lang.reflect.Field;

import com.bloom.runtime.compiler.visitors.ExpressionVisitor;

public class StaticFieldRef
  extends ValueExpr
{
  public final Field fieldRef;
  
  public StaticFieldRef(Field f)
  {
    super(ExprCmd.STATIC_FIELD);
    this.fieldRef = f;
  }
  
  public <T> Expr visit(ExpressionVisitor<T> visitor, T params)
  {
    return visitor.visitStaticFieldRef(this, params);
  }
  
  public boolean equals(Object other)
  {
    if (this == other) {
      return true;
    }
    if (!(other instanceof StaticFieldRef)) {
      return false;
    }
    StaticFieldRef o = (StaticFieldRef)other;
    return (super.equals(o)) && (this.fieldRef.equals(o.fieldRef));
  }
  
  public int hashCode()
  {
    return toString().hashCode();
  }
  
  public String toString()
  {
    return exprToString() + this.fieldRef;
  }
  
  public Class<?> getType()
  {
    return this.fieldRef.getType();
  }
}
