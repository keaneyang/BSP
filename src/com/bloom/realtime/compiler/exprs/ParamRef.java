package com.bloom.runtime.compiler.exprs;

import org.apache.commons.lang.builder.HashCodeBuilder;

import com.bloom.runtime.compiler.CompilerUtils;
import com.bloom.runtime.compiler.CompilerUtils.ParamTypeUndefined;
import com.bloom.runtime.compiler.visitors.ExpressionVisitor;

public class ParamRef
  extends ValueExpr
{
  private final String paramName;
  private int index;
  private Class<?> expectedType;
  
  public ParamRef(String pname)
  {
    super(ExprCmd.PARAMETER);
    this.paramName = pname;
    this.index = -1;
  }
  
  public Class<?> getType()
  {
    return CompilerUtils.ParamTypeUndefined.class;
  }
  
  public <T> Expr visit(ExpressionVisitor<T> visitor, T params)
  {
    return visitor.visitParamRef(this, params);
  }
  
  public boolean isConst()
  {
    return false;
  }
  
  public String toString()
  {
    return exprToString() + ":" + this.paramName;
  }
  
  public int getIndex()
  {
    return this.index;
  }
  
  public void setIndex(int idx)
  {
    this.index = idx;
  }
  
  public String getName()
  {
    return this.paramName;
  }
  
  public void setExpectedType(Class<?> type)
  {
    if (CompilerUtils.isParam(type)) {
      throw new RuntimeException("Paramter <" + this.paramName + "> has no type inferred");
    }
    if (this.expectedType == null) {
      this.expectedType = type;
    } else if (this.expectedType.isAssignableFrom(type)) {
      this.expectedType = type;
    } else {
      throw new RuntimeException("Paramter <" + this.paramName + "> is referred twice with incompatible types <" + type.getCanonicalName() + "> and <" + this.expectedType.getCanonicalName() + ">");
    }
  }
  
  public Class<?> getExpectedType()
  {
    return this.expectedType;
  }
  
  public boolean equals(Object other)
  {
    if (this == other) {
      return true;
    }
    if (!(other instanceof ParamRef)) {
      return false;
    }
    ParamRef o = (ParamRef)other;
    return (super.equals(o)) && (this.index == o.index);
  }
  
  public int hashCode()
  {
    return new HashCodeBuilder().append(super.hashCode()).append(this.index).toHashCode();
  }
}
