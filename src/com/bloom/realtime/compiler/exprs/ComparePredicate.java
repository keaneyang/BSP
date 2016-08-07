package com.bloom.runtime.compiler.exprs;

import java.util.List;
import java.util.ListIterator;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.bloom.runtime.compiler.visitors.ExpressionVisitor;

public class ComparePredicate
  extends Predicate
{
  public final List<ValueExpr> args;
  
  public ComparePredicate(ExprCmd op, List<ValueExpr> args)
  {
    super(op);
    assert (args != null);
    this.args = args;
  }
  
  public String toString()
  {
    return exprToString() + this.args;
  }
  
  public <T> Expr visit(ExpressionVisitor<T> visitor, T params)
  {
    return visitor.visitComparePredicate(this, params);
  }
  
  public List<? extends Expr> getArgs()
  {
    return this.args;
  }
  
  public <T> void visitArgs(ExpressionVisitor<T> v, T params)
  {
    ListIterator<ValueExpr> it = this.args.listIterator();
    while (it.hasNext())
    {
      ValueExpr e = (ValueExpr)it.next();
      ValueExpr enew = (ValueExpr)v.visitExpr(e, params);
      if (enew != null) {
        it.set(enew);
      }
    }
  }
  
  public boolean equals(Object other)
  {
    if (this == other) {
      return true;
    }
    if (!(other instanceof ComparePredicate)) {
      return false;
    }
    return super.equals(other);
  }
  
  public int hashCode()
  {
    return new HashCodeBuilder().append(super.hashCode()).toHashCode();
  }
}
