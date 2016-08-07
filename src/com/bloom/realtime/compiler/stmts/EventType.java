package com.bloom.runtime.compiler.stmts;

import java.util.List;

public class EventType
{
  public final String typeName;
  public final List<String> keyFields;
  
  public EventType(String typeName, List<String> keyFields)
  {
    this.typeName = typeName;
    this.keyFields = keyFields;
  }
  
  public String toString()
  {
    return this.typeName + " " + this.keyFields;
  }
}
