package com.bloom.runtime.compiler.exprs;

import java.io.Serializable;

public abstract class Predicate
  extends Expr
  implements Serializable
{
  public Predicate(ExprCmd op)
  {
    super(op);
  }
  
  public Class<?> getType()
  {
    return Boolean.TYPE;
  }
}
