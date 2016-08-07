package com.bloom.runtime.compiler.custom;

import java.util.ArrayList;
import java.util.List;

import com.bloom.runtime.compiler.AST;
import com.bloom.runtime.compiler.exprs.CastToStringOperation;
import com.bloom.runtime.compiler.exprs.DataSetRef;
import com.bloom.runtime.compiler.exprs.ExprCmd;
import com.bloom.runtime.compiler.exprs.FieldRef;
import com.bloom.runtime.compiler.exprs.FuncCall;
import com.bloom.runtime.compiler.exprs.LogicalPredicate;
import com.bloom.runtime.compiler.exprs.Predicate;
import com.bloom.runtime.compiler.exprs.ValueExpr;
import com.bloom.runtime.compiler.select.DataSet;
import com.bloom.runtime.compiler.select.ExprGenerator;
import com.bloom.runtime.compiler.select.ExprValidator;
import com.bloom.runtime.compiler.select.Var;
import com.bloom.runtime.compiler.visitors.ExprValidationVisitor;

public class AnyAttrLike
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
    List<ValueExpr> args = f.getFuncArgs();
    DataSetRef ds = (DataSetRef)args.get(0);
    ValueExpr pat = ExprValidationVisitor.compilePattern((ValueExpr)args.get(1));
    List<Predicate> preds = new ArrayList();
    for (FieldRef fld : ds.getDataSet().makeListOfAllFields())
    {
      ValueExpr cfld = new CastToStringOperation(fld);
      Predicate like = AST.NewLikeExpr(cfld, false, pat);
      preds.add(like);
    }
    Predicate p = new LogicalPredicate(ExprCmd.OR, preds);
    
    return ctx.getVar(p).getName();
  }
}
