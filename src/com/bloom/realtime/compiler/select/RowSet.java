package com.bloom.runtime.compiler.select;

import java.util.List;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.bloom.runtime.Pair;
import com.bloom.runtime.compiler.exprs.FieldRef;
import com.bloom.runtime.compiler.exprs.ValueExpr;
import com.bloom.runtime.compiler.exprs.VirtFieldRef;
import com.bloom.runtime.utils.NamePolicy;

public class RowSet
{
  private final Class<?> type;
  private final FieldAccessor fieldAccessor;
  private final RowAccessor rowAccessor;
  private final FieldFactory fieldFactory;
  private final List<Pair<String, Class<?>>> virtFields;
  
  public RowSet(Class<?> type, FieldAccessor fieldAccessor, RowAccessor rowAccessor, FieldFactory fieldFactory, List<Pair<String, Class<?>>> virtFields)
  {
    this.type = type;
    this.fieldAccessor = fieldAccessor;
    this.rowAccessor = rowAccessor;
    this.fieldFactory = fieldFactory;
    this.virtFields = virtFields;
  }
  
  public final Class<?> getJavaType()
  {
    return this.type;
  }
  
  public final String getTypeName()
  {
    return this.type.getCanonicalName();
  }
  
  public final String rsToString()
  {
    StringBuilder sb = new StringBuilder(getJavaType().getName());
    sb.append(" [\n");
    String sep = "";
    for (Pair<String, Class<?>> p : getAllTypeFields())
    {
      String nameAndType = String.format("\t%-15s %s", new Object[] { p.first, ((Class)p.second).getSimpleName() });
      
      sb.append(sep + nameAndType);
      sep = ",\n";
    }
    sb.append("\n]");
    return sb.toString();
  }
  
  public String toString()
  {
    return "RowSet(" + rsToString() + ")";
  }
  
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RowSet)) {
      return false;
    }
    RowSet other = (RowSet)o;
    return (this.type == other.type) && (this.fieldFactory == other.fieldFactory) && (this.rowAccessor == other.rowAccessor) && (this.fieldAccessor == other.fieldAccessor);
  }
  
  public int hashCode()
  {
    return new HashCodeBuilder().append(this.type.hashCode()).append(this.fieldFactory.hashCode()).append(this.rowAccessor.hashCode()).append(this.fieldAccessor.hashCode()).toHashCode();
  }
  
  public String genRowAccessor(String obj)
  {
    return this.rowAccessor.genRowAccessor(obj);
  }
  
  public String genFieldAccessor(String obj, VirtFieldRef f)
  {
    return this.fieldAccessor.genFieldAccessor(obj, f);
  }
  
  public List<Pair<String, Class<?>>> getAllTypeFields()
  {
    List<Pair<String, Class<?>>> ret = this.fieldFactory.getAllTypeFieldsImpl();
    if (this.virtFields != null) {
      ret.addAll(this.virtFields);
    }
    return ret;
  }
  
  public List<FieldRef> makeListOfAllFields(ValueExpr expr)
  {
    return this.fieldFactory.makeListOfAllFields(expr, this.fieldAccessor);
  }
  
  public FieldRef makeFieldRef(ValueExpr expr, String fieldName)
    throws NoSuchFieldException
  {
    try
    {
      return this.fieldFactory.makeFieldRefImpl(expr, this.fieldAccessor, fieldName);
    }
    catch (NoSuchFieldException e)
    {
      if (this.virtFields != null) {
        for (Pair<String, Class<?>> p : this.virtFields)
        {
          String fname = (String)p.first;
          if (NamePolicy.isEqual(fname, fieldName)) {
            return new VirtFieldRef(expr, fname, (Class)p.second, -1, this.fieldAccessor);
          }
        }
      }
      throw e;
    }
  }
}
