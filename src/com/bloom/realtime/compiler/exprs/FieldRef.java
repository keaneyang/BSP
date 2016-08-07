package com.bloom.runtime.compiler.exprs;

import java.util.List;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.bloom.runtime.compiler.AST;
import com.bloom.runtime.compiler.visitors.ExpressionVisitor;
import com.bloom.runtime.utils.NamePolicy;

public abstract class FieldRef
  extends Operation
{
  private final String name;
  
  public static FieldRef createUnresolvedFieldRef(ValueExpr expr, String name)
  {
    new FieldRef(expr, name)
    {
      public Class<?> getType()
      {
        return Object.class;
      }
      
      public String genFieldAccess(String obj)
      {
        throw new RuntimeException("internal error: access unresolved field");
      }
    };
  }
  
  protected FieldRef(ValueExpr expr, String name)
  {
    super(ExprCmd.FIELD, AST.NewList(expr));
    this.name = name;
  }
  
  public String toString()
  {
    return exprToString() + this.name;
  }
  
  public <T> Expr visit(ExpressionVisitor<T> visitor, T params)
  {
    return visitor.visitFieldRef(this, params);
  }
  
  public ValueExpr getExpr()
  {
    return (ValueExpr)this.args.get(0);
  }
  
  public boolean equals(Object other)
  {
    if (this == other) {
      return true;
    }
    if (!(other instanceof FieldRef)) {
      return false;
    }
    FieldRef o = (FieldRef)other;
    return (super.equals(o)) && (NamePolicy.isEqual(this.name, o.name));
  }
  
  public int hashCode()
  {
    return new HashCodeBuilder().append(this.name).append(super.hashCode()).toHashCode();
  }
  
  public abstract Class<?> getType();
  
  public abstract String genFieldAccess(String paramString);
  
  public boolean isStatic()
  {
    return false;
  }
  
  public String getName()
  {
    return this.name;
  }
}
