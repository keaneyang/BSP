package com.bloom.runtime.compiler.exprs;

public class Case
{
  public Expr cond;
  public ValueExpr expr;
  
  public Case(Expr cond, ValueExpr expr)
  {
    this.cond = cond;
    this.expr = expr;
  }
  
  public String toString()
  {
    return "WHEN " + this.cond + " THEN " + this.expr;
  }
}
