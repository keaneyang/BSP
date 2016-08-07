package com.bloom.runtime.compiler.exprs;

import com.fasterxml.jackson.databind.JsonNode;

public class JsonFieldRef
  extends FieldRef
{
  public JsonFieldRef(ValueExpr expr, String name)
  {
    super(expr, name);
  }
  
  public Class<?> getType()
  {
    return JsonNode.class;
  }
  
  public String genFieldAccess(String obj)
  {
    return obj + ".get(\"" + getName() + "\")";
  }
}
