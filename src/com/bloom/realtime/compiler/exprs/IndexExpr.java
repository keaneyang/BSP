package com.bloom.runtime.compiler.exprs;

import com.bloom.runtime.compiler.AST;
import com.bloom.runtime.compiler.visitors.ExpressionVisitor;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

public class IndexExpr
  extends Operation
{
  public IndexExpr(ValueExpr index, ValueExpr expr)
  {
    super(ExprCmd.INDEX, AST.NewList(expr, index));
  }
  
  public ValueExpr getExpr()
  {
    return (ValueExpr)this.args.get(0);
  }
  
  public ValueExpr getIndex()
  {
    return (ValueExpr)this.args.get(1);
  }
  
  public void setIndex(ValueExpr index)
  {
    this.args.set(1, index);
  }
  
  public <T> Expr visit(ExpressionVisitor<T> visitor, T params)
  {
    return visitor.visitIndexExpr(this, params);
  }
  
  public String toString()
  {
    return exprToString() + getExpr() + "[" + getIndex() + "]";
  }
  
  private Class<?> getBaseType()
  {
    return getExpr().getType();
  }
  
  public Class<?> getType()
  {
    if (baseIsJsonNode()) {
      return JsonNode.class;
    }
    Class<?> ta = getBaseType();
    Class<?> t = ta == null ? null : ta.getComponentType();
    return t;
  }
  
  public boolean baseIsJsonNode()
  {
    Class<?> t = getBaseType();
    return JsonNode.class.isAssignableFrom(t);
  }
  
  public boolean baseIsArray()
  {
    Class<?> t = getBaseType();
    return (t.isArray()) || (JsonNode.class.isAssignableFrom(t));
  }
}
