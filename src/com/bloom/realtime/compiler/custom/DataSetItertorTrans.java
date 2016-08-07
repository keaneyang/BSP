package com.bloom.runtime.compiler.custom;

import java.util.List;

import com.bloom.runtime.compiler.exprs.DataSetRef;
import com.bloom.runtime.compiler.exprs.FuncCall;
import com.bloom.runtime.compiler.exprs.ValueExpr;
import com.bloom.runtime.compiler.select.DataSet;
import com.bloom.runtime.compiler.select.ExprGenerator;
import com.bloom.runtime.compiler.select.ExprValidator;

public class DataSetItertorTrans
  implements CustomFunctionTranslator
{
  public ValueExpr validate(ExprValidator ctx, FuncCall f)
  {
    List<ValueExpr> args = f.getFuncArgs();
    ValueExpr arg = (ValueExpr)args.get(0);
    if (!(arg instanceof DataSetRef)) {
      ctx.error("function expects argument of <data source reference> type ", arg);
    }
    return null;
  }
  
  public String generate(ExprGenerator ctx, FuncCall f)
  {
    DataSetRef dsref = (DataSetRef)f.getFuncArgs().get(0);
    return "getContext().makeSnapshotIterator(" + dsref.getDataSet().getID() + ")";
  }
}
