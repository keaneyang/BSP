package com.bloom.runtime.compiler.exprs;

import com.bloom.runtime.compiler.visitors.ExpressionVisitor;
import com.bloom.runtime.utils.StringUtils;
import java.util.List;
import java.util.ListIterator;

public abstract class Operation
  extends ValueExpr
{
  public final List<ValueExpr> args;
  
  public Operation(ExprCmd op, List<ValueExpr> args)
  {
    super(op);
    assert (args != null);
    this.args = args;
  }
  
  protected String argsToString()
  {
    return StringUtils.join(this.args);
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
  
  public String toString()
  {
    return exprToString() + this.args;
  }
}
