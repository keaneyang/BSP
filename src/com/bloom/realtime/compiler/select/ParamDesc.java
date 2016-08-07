package com.bloom.runtime.compiler.select;

import java.io.Serializable;

public class ParamDesc
  implements Serializable
{
  private static final long serialVersionUID = -5558193996272771638L;
  public String paramName;
  public int index;
  public Class<?> expectedType;
  
  public ParamDesc() {}
  
  public ParamDesc(String paramName, int index, Class<?> expectedType)
  {
    this.paramName = paramName;
    this.index = index;
    this.expectedType = expectedType;
  }
}
