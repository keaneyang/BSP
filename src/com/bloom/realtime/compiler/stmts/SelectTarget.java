package com.bloom.runtime.compiler.stmts;

import java.io.Serializable;

import com.bloom.runtime.compiler.exprs.ValueExpr;

public class SelectTarget
  implements Serializable
{
  public final ValueExpr expr;
  public final String alias;
  
  public SelectTarget(ValueExpr expr, String alias)
  {
    this.expr = expr;
    this.alias = alias;
  }
  
  public String toString()
  {
    if (this.expr == null) {
      return "*";
    }
    return "" + this.expr + (this.alias == null ? "" : new StringBuilder().append(" AS ").append(this.alias).toString());
  }
}
