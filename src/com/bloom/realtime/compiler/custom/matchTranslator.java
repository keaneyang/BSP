package com.bloom.runtime.compiler.custom;

import java.util.List;
import java.util.regex.Pattern;

import com.bloom.runtime.BuiltInFunc;
import com.bloom.runtime.compiler.exprs.Expr;
import com.bloom.runtime.compiler.exprs.FuncCall;
import com.bloom.runtime.compiler.exprs.ValueExpr;
import com.bloom.runtime.compiler.select.ExprGenerator;
import com.bloom.runtime.compiler.select.ExprValidator;
import com.bloom.runtime.compiler.select.Var;

public class matchTranslator
  implements CustomFunctionTranslator
{
  public ValueExpr validate(ExprValidator ctx, FuncCall f)
  {
    return null;
  }
  
  public String generate(ExprGenerator ctx, FuncCall f)
  {
    List<ValueExpr> args = f.getFuncArgs();
    Var arg1 = ctx.getVar((Expr)args.get(0));
    ValueExpr e = (ValueExpr)args.get(1);
    Var arg2 = ctx.getVar(e);
    String cn = BuiltInFunc.class.getName();
    if (e.isConst())
    {
      String pt = Pattern.class.getName();
      arg2 = ctx.addStaticExpr(Pattern.class, pt + ".compile(" + arg2 + ")");
    }
    return cn + ".match2impl(" + arg1 + ", " + arg2 + ")";
  }
}
