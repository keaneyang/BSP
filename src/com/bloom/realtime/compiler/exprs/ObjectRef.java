package com.bloom.runtime.compiler.exprs;

import com.bloom.runtime.compiler.select.ExprValidator;
import com.bloom.runtime.compiler.visitors.ExpressionVisitor;

public class ObjectRef
  extends ValueExpr
{
  public String name;
  private ValueExpr resolved;
  private ExprValidator ctx;
  
  public ObjectRef(String name)
  {
    super(ExprCmd.VAR);
    this.name = name;
    this.resolved = null;
    this.ctx = null;
  }
  
  public void setContext(ExprValidator ctx)
  {
    this.ctx = ctx;
  }
  
  public <T> Expr visit(ExpressionVisitor<T> visitor, T params)
  {
    if (this.resolved != null) {
      return this.resolved.visit(visitor, params);
    }
    return visitor.visitObjectRef(this, params);
  }
  
  public Expr resolve()
  {
    if ((this.resolved == null) && 
      (this.ctx != null))
    {
      this.resolved = this.ctx.findStaticVariableOrClass(this.name);
      if (this.resolved == null) {
        this.ctx.error("cannot resolve object " + this.name, this);
      }
    }
    return this.resolved;
  }
  
  public Class<?> getType()
  {
    Expr resolveExpr = resolve();
    if (resolveExpr != null) {
      return resolveExpr.getType();
    }
    return null;
  }
  
  public boolean equals(Object other)
  {
    if (this == other) {
      return true;
    }
    if (!(other instanceof ObjectRef)) {
      return false;
    }
    ObjectRef o = (ObjectRef)other;
    return (super.equals(o)) && (this.name.equals(o.name));
  }
  
  public String toString()
  {
    return exprToString() + this.name;
  }
}
