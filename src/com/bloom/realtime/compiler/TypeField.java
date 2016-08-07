package com.bloom.runtime.compiler;

import java.io.Serializable;

public class TypeField
  implements Serializable
{
  public String fieldName;
  public TypeName fieldType;
  public boolean isPartOfKey;
  
  public TypeField() {}
  
  public TypeField(String name, TypeName type, boolean iskey)
  {
    this.fieldName = name;
    this.fieldType = type;
    this.isPartOfKey = iskey;
  }
  
  public String toString()
  {
    return this.fieldName + " " + this.fieldType + (this.isPartOfKey ? " KEY" : "");
  }
}
