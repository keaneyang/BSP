package com.bloom.runtime.compiler;

import java.io.Serializable;

public class TypeName
  implements Serializable
{
  public String name;
  public int array_dimensions;
  
  public TypeName() {}
  
  public TypeName(String name, int array_dimensions)
  {
    this.name = name;
    this.array_dimensions = array_dimensions;
  }
  
  public String toString()
  {
    return this.name + new String(new char[this.array_dimensions]).replace("\000", "[]");
  }
}
