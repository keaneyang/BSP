package com.bloom.runtime.compiler.custom;

import com.bloom.runtime.compiler.exprs.FuncCall;
import com.bloom.runtime.compiler.exprs.ValueExpr;
import com.bloom.runtime.compiler.select.ExprGenerator;
import com.bloom.runtime.compiler.select.ExprValidator;

public abstract interface CustomFunctionTranslator
{
  public abstract ValueExpr validate(ExprValidator paramExprValidator, FuncCall paramFuncCall);
  
  public abstract String generate(ExprGenerator paramExprGenerator, FuncCall paramFuncCall);
}

