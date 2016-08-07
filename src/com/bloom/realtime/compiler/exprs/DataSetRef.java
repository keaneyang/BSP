package com.bloom.runtime.compiler.exprs;

import org.apache.commons.lang.builder.HashCodeBuilder;

import com.bloom.runtime.compiler.select.DataSet;
import com.bloom.runtime.compiler.visitors.ExpressionVisitor;

public abstract class DataSetRef
  extends ValueExpr
{
  private int index;
  
  public DataSetRef(ExprCmd kind, int index)
  {
    super(kind);
    this.index = index;
  }
  
  public abstract DataSet getDataSet();
  
  public static DataSetRef makeDataSetRef(final DataSet dataSet)
  {
    new DataSetRef(ExprCmd.DATASET, 0)
    {
      public DataSet getDataSet()
      {
        return dataSet;
      }
    };
  }
  
  public <T> Expr visit(ExpressionVisitor<T> visitor, T params)
  {
    return visitor.visitDataSetRef(this, params);
  }
  
  public boolean equals(Object other)
  {
    if (this == other) {
      return true;
    }
    if (!(other instanceof DataSetRef)) {
      return false;
    }
    DataSetRef o = (DataSetRef)other;
    return (super.equals(o)) && (getDataSet().equals(o.getDataSet())) && (this.index == o.index);
  }
  
  public int hashCode()
  {
    return new HashCodeBuilder().append(super.hashCode()).append(this.index).append(getDataSet().getFullName()).toHashCode();
  }
  
  public String toString()
  {
    return exprToString() + getDataSet().getName();
  }
  
  public boolean isConst()
  {
    return false;
  }
  
  public Class<?> getType()
  {
    return getDataSet().getJavaType();
  }
  
  public FieldRef getField(String fieldName)
    throws NoSuchFieldException, SecurityException
  {
    return getDataSet().makeFieldRef(this, fieldName);
  }
  
  public void setIndex(int idx)
  {
    this.index = idx;
  }
  
  public int getIndex()
  {
    return this.index;
  }
}
