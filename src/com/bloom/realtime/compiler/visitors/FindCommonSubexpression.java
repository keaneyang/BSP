package com.bloom.runtime.compiler.visitors;

import com.bloom.runtime.compiler.exprs.Expr;
import com.bloom.runtime.compiler.exprs.FuncCall;

public abstract class FindCommonSubexpression
  extends ExpressionVisitorDefaultImpl<Object>
{
  private final Rewriter rewriter;
  private boolean inAggFunc = false;
  
  public FindCommonSubexpression(Rewriter r)
  {
    this.rewriter = r;
  }
  
  Expr rewriteExpr(Expr e)
  {
    return this.rewriter.rewriteExpr(e);
  }
  
  public Expr visitExprDefault(Expr e, Object params)
  {
    e.visitArgs(this, null);
    return rewriteExpr(e);
  }
  
  public <T> T rewrite(Expr e)
  {
    T result = visitExpr(e, null);
    return result;
  }
  
  public Expr visitFuncCall(FuncCall funcCall, Object params)
  {
    if (funcCall.getAggrDesc() != null)
    {
      this.inAggFunc = true;
      funcCall.visitArgs(this, params);
      this.inAggFunc = false;
      FuncCall fc = (FuncCall)rewriteExpr(funcCall);
      this.rewriter.addAggrExpr(fc);
      return fc;
    }
    return visitExprDefault(funcCall, params);
  }
  
  public boolean isInAggFunc()
  {
    return this.inAggFunc;
  }
  
  public static abstract interface Rewriter
  {
    public abstract Expr rewriteExpr(Expr paramExpr);
    
    public abstract void addAggrExpr(FuncCall paramFuncCall);
  }
}
