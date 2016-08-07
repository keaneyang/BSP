package com.bloom.runtime.compiler.exprs;

import java.util.List;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.bloom.runtime.compiler.AST;
import com.bloom.runtime.compiler.TypeName;
import com.bloom.runtime.compiler.visitors.ExpressionVisitor;

public class InstanceOfPredicate
  extends ComparePredicate
{
  public final TypeName checktype;
  
  public InstanceOfPredicate(ValueExpr expr, TypeName type)
  {
    super(ExprCmd.INSTANCEOF, AST.NewList(expr));
    this.checktype = type;
  }
  
  public ValueExpr getExpr()
  {
    return (ValueExpr)this.args.get(0);
  }
  
  public String toString()
  {
    return exprToString() + getExpr() + " instanceof " + this.checktype;
  }
  
  public <T> Expr visit(ExpressionVisitor<T> visitor, T params)
  {
    return visitor.visitInstanceOfPredicate(this, params);
  }
  
  public boolean equals(Object other)
  {
    if (this == other) {
      return true;
    }
    if (!(other instanceof InstanceOfPredicate)) {
      return false;
    }
    InstanceOfPredicate o = (InstanceOfPredicate)other;
    return (super.equals(other)) && (this.checktype.equals(o.checktype));
  }
  
  public int hashCode()
  {
    return new HashCodeBuilder().append(this.checktype).append(super.hashCode()).toHashCode();
  }
}
