package com.bloom.runtime;

import java.util.List;
import java.util.Map;

import com.bloom.runtime.compiler.TypeDefOrName;
import com.bloom.runtime.compiler.TypeField;

public class PropablePropertySet
  extends TypeDefOrName
{
  Map<String, Object> propertySet;
  
  public PropablePropertySet(String typeName, List<TypeField> typeDef)
  {
    super(typeName, typeDef);
  }
  
  public void setPropertySet(Map<String, Object> prop)
  {
    this.propertySet = prop;
  }
  
  public Map<String, Object> getPropertySet()
  {
    return this.propertySet;
  }
}
