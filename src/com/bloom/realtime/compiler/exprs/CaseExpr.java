package com.bloom.runtime.compiler.exprs;

import java.util.ArrayList;
import java.util.List;

import com.bloom.runtime.compiler.visitors.ExpressionVisitor;

public class CaseExpr
  extends ValueExpr
{
  private Class<?> resultType;
  private List<Expr> cachedargs = null;
  public ValueExpr selector;
  public final List<Case> cases;
  public ValueExpr else_expr;
  
  public CaseExpr(ValueExpr selector, List<Case> cases, ValueExpr else_expr)
  {
    super(ExprCmd.CASE);
    this.selector = selector;
    this.cases = cases;
    this.else_expr = else_expr;
  }
  
  public String toString()
  {
    String res = "";
    if (this.selector != null) {
      res = res + this.selector;
    }
    res = res + this.cases;
    if (this.else_expr != null) {
      res = res + this.else_expr;
    }
    return exprToString() + res + " END";
  }
  
  public <T> Expr visit(ExpressionVisitor<T> visitor, T params)
  {
    return visitor.visitCase(this, params);
  }
  
  public List<? extends Expr> getArgs()
  {
    if (this.cachedargs == null)
    {
      List<Expr> args = new ArrayList();
      if (this.selector != null) {
        args.add(this.selector);
      }
      for (Case c : this.cases)
      {
        args.add(c.cond);
        args.add(c.expr);
      }
      if (this.else_expr != null) {
        args.add(this.else_expr);
      }
      this.cachedargs = args;
    }
    return this.cachedargs;
  }
  
  public <T> void visitArgs(ExpressionVisitor<T> v, T params)
  {
    this.cachedargs = null;
    if (this.selector != null)
    {
      ValueExpr enew = (ValueExpr)v.visitExpr(this.selector, params);
      if (enew != null) {
        this.selector = enew;
      }
    }
    for (Case c : this.cases)
    {
      Expr cnew = v.visitExpr(c.cond, params);
      if (cnew != null) {
        c.cond = cnew;
      }
      ValueExpr enew = (ValueExpr)v.visitExpr(c.expr, params);
      if (enew != null) {
        c.expr = enew;
      }
    }
    if (this.else_expr != null)
    {
      ValueExpr enew = (ValueExpr)v.visitExpr(this.else_expr, params);
      if (enew != null) {
        this.else_expr = enew;
      }
    }
  }
  
  public Class<?> getType()
  {
    assert (this.resultType != null);
    return this.resultType;
  }
  
  public void setType(Class<?> resType)
  {
    assert (resType != null);
    this.resultType = resType;
  }
}
