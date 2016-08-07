package com.bloom.runtime.compiler.exprs;

import java.util.List;

import com.bloom.runtime.compiler.visitors.ExpressionVisitor;

public class PredicateAsValueExpr
  extends ValueExpr
{
  private final Predicate p;
  
  public PredicateAsValueExpr(Predicate p)
  {
    super(p.op);
    this.p = p;
  }
  
  public Class<?> getType()
  {
    return this.p.getType();
  }
  
  public <T> Expr visit(ExpressionVisitor<T> visitor, T params)
  {
    Expr res = this.p.visit(visitor, params);
    if (res != null)
    {
      PredicateAsValueExpr wrapper = new PredicateAsValueExpr((Predicate)res);
      res = wrapper.setOriginalExpr(res);
    }
    return res;
  }
  
  public List<? extends Expr> getArgs()
  {
    return this.p.getArgs();
  }
  
  public <T> void visitArgs(ExpressionVisitor<T> v, T params)
  {
    this.p.visitArgs(v, params);
  }
  
  public boolean equals(Object other)
  {
    if (this == other) {
      return true;
    }
    if ((other instanceof PredicateAsValueExpr))
    {
      PredicateAsValueExpr o = (PredicateAsValueExpr)other;
      return this.p.equals(o.p);
    }
    if ((other instanceof Predicate))
    {
      Predicate o = (Predicate)other;
      return this.p.equals(o);
    }
    return false;
  }
  
  public int hashCode()
  {
    return this.p.hashCode();
  }
  
  public String toString()
  {
    return this.p.toString();
  }
  
  public boolean isConst()
  {
    return this.p.isConst();
  }
}
