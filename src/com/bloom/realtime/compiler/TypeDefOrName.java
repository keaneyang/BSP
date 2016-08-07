package com.bloom.runtime.compiler;

import java.util.List;

public class TypeDefOrName
{
  public final String typeName;
  public final List<TypeField> typeDef;
  public String extendsTypeName = null;
  
  public TypeDefOrName(String typeName, List<TypeField> typeDef)
  {
    this.typeName = typeName;
    this.typeDef = typeDef;
  }
  
  public void setExtendsTypeName(String extendsTypeName)
  {
    this.extendsTypeName = extendsTypeName;
  }
  
  public String toString()
  {
    return "typedef " + this.typeName + " " + this.typeDef + (this.extendsTypeName != null ? " extends " + this.extendsTypeName : "");
  }
}
