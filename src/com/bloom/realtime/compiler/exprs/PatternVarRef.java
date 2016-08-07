package com.bloom.runtime.compiler.exprs;

import org.apache.commons.lang.builder.HashCodeBuilder;

import com.bloom.runtime.compiler.patternmatch.PatternVariable;
import com.bloom.runtime.compiler.select.DataSet;
import com.bloom.runtime.compiler.visitors.ExpressionVisitor;

public class PatternVarRef
  extends DataSetRef
{
  private final PatternVariable var;
  
  public PatternVarRef(PatternVariable var)
  {
    super(ExprCmd.PATTERN_VAR, -1);
    this.var = var;
  }
  
  public <T> Expr visit(ExpressionVisitor<T> visitor, T params)
  {
    return visitor.visitPatternVarRef(this, params);
  }
  
  public boolean equals(Object other)
  {
    if (this == other) {
      return true;
    }
    if (!(other instanceof PatternVarRef)) {
      return false;
    }
    PatternVarRef o = (PatternVarRef)other;
    return (super.equals(o)) && (this.var.equals(o.var));
  }
  
  public int hashCode()
  {
    return new HashCodeBuilder().append(super.hashCode()).append(this.var.toString()).toHashCode();
  }
  
  public String toString()
  {
    return exprToString() + this.var.getName();
  }
  
  public Class<?> getType()
  {
    return this.var.getJavaType();
  }
  
  public int getVarId()
  {
    return this.var.getId();
  }
  
  public DataSet getDataSet()
  {
    return this.var.getDataSet();
  }
}
