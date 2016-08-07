package com.bloom.runtime.compiler.custom;

import java.util.List;

import com.bloom.runtime.compiler.exprs.Constant;
import com.bloom.runtime.compiler.exprs.ExprCmd;
import com.bloom.runtime.compiler.exprs.FuncCall;
import com.bloom.runtime.compiler.exprs.RowSetRef;
import com.bloom.runtime.compiler.exprs.ValueExpr;
import com.bloom.runtime.compiler.select.DataSet;
import com.bloom.runtime.compiler.select.ExprGenerator;
import com.bloom.runtime.compiler.select.ExprValidator;

public class AccessPrevEventInBuffer
  implements CustomFunctionTranslator
{
  public ValueExpr validate(ExprValidator ctx, FuncCall f)
  {
    List<ValueExpr> args = f.getFuncArgs();
    if (!args.isEmpty())
    {
      ValueExpr arg = (ValueExpr)args.get(0);
      if (arg.op != ExprCmd.INT) {
        ctx.error("must be integer constant", arg);
      }
    }
    DataSet ds = ctx.getDefaultDataSet();
    if (ds == null) {
      ctx.error("function can be called only in expression defining pattern matching variable", f);
    }
    return new RowSetRef(f, ds);
  }
  
  public String generate(ExprGenerator ctx, FuncCall f)
  {
    List<ValueExpr> args = f.getFuncArgs();
    int index;
    int index;
    if (!args.isEmpty())
    {
      ValueExpr arg = (ValueExpr)args.get(0);
      index = ((Integer)((Constant)arg).value).intValue();
    }
    else
    {
      index = 1;
    }
    ctx.setPrevEventIndex(index);
    return "ctx.getEventAt(pos, " + index + ")";
  }
}
