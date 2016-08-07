package com.bloom.runtime.compiler.exprs;

import org.apache.commons.lang.builder.HashCodeBuilder;

import com.bloom.runtime.compiler.CompilerUtils;
import com.bloom.runtime.compiler.select.FieldAccessor;

public class VirtFieldRef
  extends FieldRef
{
  private final Class<?> type;
  private final int index;
  private final FieldAccessor fieldAccessor;
  
  public VirtFieldRef(ValueExpr expr, String name, Class<?> type, int index, FieldAccessor fieldAccessor)
  {
    super(expr, name);
    this.type = (type.isPrimitive() ? CompilerUtils.getBoxingType(type) : type);
    this.index = index;
    this.fieldAccessor = fieldAccessor;
  }
  
  public boolean equals(Object other)
  {
    if (this == other) {
      return true;
    }
    if (!(other instanceof VirtFieldRef)) {
      return false;
    }
    VirtFieldRef o = (VirtFieldRef)other;
    return (super.equals(o)) && (this.type.equals(o.type)) && (this.index == o.index);
  }
  
  public int hashCode()
  {
    return new HashCodeBuilder().append(super.hashCode()).append(this.index).append(this.type).toHashCode();
  }
  
  public Class<?> getType()
  {
    return this.type;
  }
  
  public int getIndex()
  {
    return this.index;
  }
  
  public String genFieldAccess(String obj)
  {
    assert (!this.type.isPrimitive());
    String fld = this.fieldAccessor.genFieldAccessor(obj, this);
    return "((" + getTypeName() + ")" + fld + ")";
  }
}
