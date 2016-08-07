package com.bloom.runtime.compiler.exprs;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import org.apache.commons.lang.builder.HashCodeBuilder;

public class ObjFieldRef
  extends FieldRef
{
  private final Field fieldRef;
  
  public ObjFieldRef(ValueExpr expr, String name, Field field)
  {
    super(expr, name);
    this.fieldRef = field;
  }
  
  public boolean equals(Object other)
  {
    if (this == other) {
      return true;
    }
    if (!(other instanceof ObjFieldRef)) {
      return false;
    }
    ObjFieldRef o = (ObjFieldRef)other;
    return (super.equals(o)) && (this.fieldRef.equals(o.fieldRef));
  }
  
  public int hashCode()
  {
    return new HashCodeBuilder().append(super.hashCode()).append(this.fieldRef.toString()).toHashCode();
  }
  
  public Class<?> getType()
  {
    return this.fieldRef.getType();
  }
  
  public boolean isStatic()
  {
    return Modifier.isStatic(this.fieldRef.getModifiers());
  }
  
  public String genFieldAccess(String obj)
  {
    return obj + "." + this.fieldRef.getName();
  }
}
