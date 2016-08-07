package com.bloom.runtime.compiler.exprs;

import java.util.List;

public class IntegerOperation
  extends NumericOperation
{
  public IntegerOperation(ExprCmd op, List<ValueExpr> args)
  {
    super(op, args);
  }
}
