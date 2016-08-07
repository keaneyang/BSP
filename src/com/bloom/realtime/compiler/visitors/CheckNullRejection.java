package com.bloom.runtime.compiler.visitors;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import com.bloom.runtime.compiler.exprs.ComparePredicate;
import com.bloom.runtime.compiler.exprs.DataSetRef;
import com.bloom.runtime.compiler.exprs.Expr;
import com.bloom.runtime.compiler.exprs.ExprCmd;
import com.bloom.runtime.compiler.exprs.InstanceOfPredicate;
import com.bloom.runtime.compiler.exprs.LogicalPredicate;
import com.bloom.runtime.compiler.exprs.Predicate;
import com.bloom.runtime.compiler.select.DataSet;

public class CheckNullRejection
  extends ExpressionVisitorDefaultImpl<Param>
{
  private List<Param> args;
  private Param ret;
  private final int innerDS;
  
  public static class Param
  {
    BitSet dataSets = new BitSet();
    boolean acceptNull;
  }
  
  public static boolean acceptNulls(Predicate p, int innerDS)
  {
    CheckNullRejection c = new CheckNullRejection(p, innerDS);
    return c.acceptNulls();
  }
  
  private CheckNullRejection(Predicate p, int innerDS)
  {
    this.args = new ArrayList();
    this.ret = new Param();
    this.innerDS = innerDS;
    visitExpr(p, this.ret);
  }
  
  private boolean acceptNulls()
  {
    return this.ret.acceptNull;
  }
  
  public Expr visitExpr(Expr e, Param param)
  {
    List<Param> xargs = new ArrayList();
    for (Expr arg : e.getArgs())
    {
      Param p = new Param();
      visitExpr(arg, p);
      xargs.add(p);
      param.dataSets.or(p.dataSets);
    }
    this.args = xargs;
    e.visit(this, param);
    return null;
  }
  
  public Expr visitDataSetRef(DataSetRef ref, Param param)
  {
    DataSet ds = ref.getDataSet();
    param.dataSets.set(ds.getID());
    return null;
  }
  
  public Expr visitLogicalPredicate(LogicalPredicate logicalPredicate, Param param)
  {
    boolean acceptNull = false;
    switch (logicalPredicate.op)
    {
    case AND: 
      acceptNull = true;
      for (Param p : this.args) {
        if (!p.acceptNull)
        {
          acceptNull = false;
          break;
        }
      }
      break;
    case OR: 
      acceptNull = false;
      for (Param p : this.args) {
        if (p.acceptNull)
        {
          acceptNull = true;
          break;
        }
      }
      break;
    case NOT: 
      acceptNull = !((Param)this.args.get(0)).acceptNull;
      break;
    default: 
      if (!$assertionsDisabled) {
        throw new AssertionError();
      }
      break;
    }
    param.acceptNull = acceptNull;
    return null;
  }
  
  public Expr visitComparePredicate(ComparePredicate comparePredicate, Param param)
  {
    if ((comparePredicate.op == ExprCmd.ISNULL) && (param.dataSets.get(this.innerDS))) {
      param.acceptNull = true;
    } else {
      param.acceptNull = (!param.dataSets.get(this.innerDS));
    }
    return null;
  }
  
  public Expr visitInstanceOfPredicate(InstanceOfPredicate ins, Param param)
  {
    param.acceptNull = (!param.dataSets.get(this.innerDS));
    return null;
  }
}
