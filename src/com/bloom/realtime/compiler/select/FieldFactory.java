package com.bloom.runtime.compiler.select;

import java.util.List;

import com.bloom.runtime.Pair;
import com.bloom.runtime.compiler.exprs.FieldRef;
import com.bloom.runtime.compiler.exprs.ValueExpr;

public abstract class FieldFactory
{
  public abstract List<Pair<String, Class<?>>> getAllTypeFieldsImpl();
  
  public abstract List<FieldRef> makeListOfAllFields(ValueExpr paramValueExpr, FieldAccessor paramFieldAccessor);
  
  public abstract FieldRef makeFieldRefImpl(ValueExpr paramValueExpr, FieldAccessor paramFieldAccessor, String paramString)
    throws NoSuchFieldException, SecurityException;
  
  public abstract String toString();
}

