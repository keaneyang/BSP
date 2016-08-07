package com.bloom.runtime.compiler.exprs;

import java.util.List;
import java.util.ListIterator;

import com.bloom.runtime.compiler.visitors.ExpressionVisitor;

public class LogicalPredicate
  extends Predicate
{
  public final List<Predicate> args;
  
  public LogicalPredicate(ExprCmd op, List<Predicate> args)
  {
    super(op);
    this.args = args;
  }
  
  public String toString()
  {
    return exprToString() + this.args;
  }
  
  public <T> Expr visit(ExpressionVisitor<T> visitor, T params)
  {
    return visitor.visitLogicalPredicate(this, params);
  }
  
  public List<? extends Expr> getArgs()
  {
    return this.args;
  }
  
  public <T> void visitArgs(ExpressionVisitor<T> v, T params)
  {
    ListIterator<Predicate> it = this.args.listIterator();
    while (it.hasNext())
    {
      Predicate p = (Predicate)it.next();
      Predicate pnew = (Predicate)v.visitExpr(p, params);
      if (pnew != null) {
        it.set(pnew);
      }
    }
  }
  
  public boolean equals(Object other)
  {
    if (this == other) {
      return true;
    }
    if (!(other instanceof LogicalPredicate)) {
      return false;
    }
    return super.equals(other);
  }
  
  public int hashCode()
  {
    return super.hashCode();
  }
}
