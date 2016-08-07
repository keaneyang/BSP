package com.bloom.runtime.compiler.stmts;

import com.bloom.runtime.compiler.exprs.ValueExpr;

public class OrderByItem
{
  public final ValueExpr expr;
  public final boolean isAscending;
  
  public OrderByItem(ValueExpr expr, boolean isAscending)
  {
    this.expr = expr;
    this.isAscending = isAscending;
  }
}
