package com.bloom.runtime.compiler.exprs;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

import com.bloom.runtime.compiler.visitors.ExpressionVisitor;

public abstract class Expr
  implements Serializable
{
  private static int nextExprID = 0;
  public final ExprCmd op;
  public final int eid;
  public Expr originalExpr;
  
  public Expr(ExprCmd op)
  {
    this.op = op;
    this.eid = (++nextExprID);
  }
  
  public String exprToString()
  {
    return this.op + "#" + this.eid + " ";
  }
  
  public String toString()
  {
    return exprToString();
  }
  
  public abstract Class<?> getType();
  
  public abstract List<? extends Expr> getArgs();
  
  public abstract <T> Expr visit(ExpressionVisitor<T> paramExpressionVisitor, T paramT);
  
  public abstract <T> void visitArgs(ExpressionVisitor<T> paramExpressionVisitor, T paramT);
  
  public boolean equals(Object other)
  {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Expr)) {
      return false;
    }
    Expr o = (Expr)other;
    return (this.op.equals(o.op)) && (getType().equals(o.getType())) && (getArgs().equals(o.getArgs()));
  }
  
  public int hashCode()
  {
    return this.op.hashCode();
  }
  
  public static Class<?>[] getSignature(Collection<? extends Expr> exprs)
  {
    Class<?>[] sig = new Class[exprs.size()];
    int i = 0;
    for (Expr e : exprs) {
      sig[(i++)] = e.getType();
    }
    return sig;
  }
  
  public boolean isTypeRef()
  {
    return false;
  }
  
  public Expr resolve()
  {
    return this;
  }
  
  public boolean isConst()
  {
    for (Expr e : getArgs()) {
      if (!e.isConst()) {
        return false;
      }
    }
    return true;
  }
  
  public Expr setOriginalExpr(Expr e)
  {
    if (e != this) {
      this.originalExpr = e;
    }
    return this;
  }
  
  public final String getTypeName()
  {
    return getType().getCanonicalName();
  }
}
