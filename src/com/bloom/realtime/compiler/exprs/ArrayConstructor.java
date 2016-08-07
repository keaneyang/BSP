package com.bloom.runtime.compiler.exprs;

import java.util.List;

import com.bloom.runtime.compiler.TypeName;

public class ArrayConstructor
  extends ConstructorCall
{
  public ArrayConstructor(TypeName type, List<ValueExpr> indexes)
  {
    super(type, indexes);
  }
  
  public String toString()
  {
    return exprToString() + this.objtype + "[" + argsToString() + "]";
  }
}
