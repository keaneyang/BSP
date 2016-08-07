package com.bloom.runtime.compiler.exprs;

import java.util.List;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.bloom.runtime.compiler.AST;
import com.bloom.runtime.compiler.select.RowSet;
import com.bloom.runtime.compiler.visitors.ExpressionVisitor;

public class RowSetRef
  extends Operation
{
  private final RowSet rowSet;
  
  public RowSetRef(ValueExpr expr, RowSet rs)
  {
    super(ExprCmd.ROWSET, AST.NewList(expr));
    this.rowSet = rs;
  }
  
  public Class<?> getType()
  {
    return this.rowSet.getJavaType();
  }
  
  public <T> Expr visit(ExpressionVisitor<T> visitor, T params)
  {
    return visitor.visitRowSetRef(this, params);
  }
  
  public boolean equals(Object other)
  {
    if (this == other) {
      return true;
    }
    if (!(other instanceof RowSetRef)) {
      return false;
    }
    RowSetRef o = (RowSetRef)other;
    return (super.equals(o)) && (this.rowSet.equals(o.rowSet));
  }
  
  public int hashCode()
  {
    return new HashCodeBuilder().append(this.rowSet.toString()).append(super.hashCode()).toHashCode();
  }
  
  public String toString()
  {
    return exprToString() + this.rowSet.toString();
  }
  
  public FieldRef getField(String fieldName)
    throws NoSuchFieldException, SecurityException
  {
    return this.rowSet.makeFieldRef(this, fieldName);
  }
  
  public RowSet getRowSet()
  {
    return this.rowSet;
  }
  
  public ValueExpr getExpr()
  {
    return (ValueExpr)this.args.get(0);
  }
}
