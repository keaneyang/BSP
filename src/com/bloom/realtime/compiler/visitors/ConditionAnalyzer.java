package com.bloom.runtime.compiler.visitors;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import com.bloom.runtime.compiler.exprs.ComparePredicate;
import com.bloom.runtime.compiler.exprs.DataSetRef;
import com.bloom.runtime.compiler.exprs.Expr;
import com.bloom.runtime.compiler.exprs.ExprCmd;
import com.bloom.runtime.compiler.exprs.LogicalPredicate;
import com.bloom.runtime.compiler.exprs.Predicate;
import com.bloom.runtime.compiler.exprs.ValueExpr;
import com.bloom.runtime.compiler.select.DataSet;
import com.bloom.runtime.compiler.select.Condition.CondExpr;
import com.bloom.runtime.compiler.select.Condition.JoinExpr;
import com.bloom.runtime.compiler.select.Join.JoinNode;

public class ConditionAnalyzer
  extends ExpressionVisitorDefaultImpl<BitSet>
{
  private final Predicate predicate;
  private int depth;
  private List<BitSet> argSets;
  private Condition.JoinExpr equijoin;
  private final BitSet datasets;
  private final Join.JoinNode joinInfo;
  
  public ConditionAnalyzer(Predicate p, Join.JoinNode jn)
  {
    this.joinInfo = jn;
    this.predicate = p;
    this.depth = 0;
    this.argSets = new ArrayList();
    this.datasets = new BitSet();
    this.equijoin = null;
    visitExpr(p, this.datasets);
  }
  
  public Condition.CondExpr getCondExpr()
  {
    if (this.equijoin != null) {
      return this.equijoin;
    }
    return new Condition.CondExpr(this.predicate, this.datasets, this.joinInfo);
  }
  
  public Expr visitExpr(Expr e, BitSet bs)
  {
    this.depth += 1;
    List<BitSet> argSets = new ArrayList();
    for (Expr arg : e.getArgs())
    {
      BitSet argbs = new BitSet();
      visitExpr(arg, argbs);
      argSets.add(argbs);
      bs.or(argbs);
    }
    this.argSets = argSets;
    e.visit(this, bs);
    this.depth -= 1;
    return null;
  }
  
  public Expr visitDataSetRef(DataSetRef ref, BitSet bs)
  {
    DataSet ds = ref.getDataSet();
    bs.set(ds.getID());
    return null;
  }
  
  public Expr visitLogicalPredicate(LogicalPredicate logicalPredicate, BitSet bs)
  {
    if ((this.depth == 1) && (logicalPredicate.op == ExprCmd.AND) && 
      (!$assertionsDisabled)) {
      throw new AssertionError("AND predicate in condition");
    }
    return null;
  }
  
  public Expr visitComparePredicate(ComparePredicate comparePredicate, BitSet bs)
  {
    if ((this.depth == 1) && (comparePredicate.op == ExprCmd.EQ) && (bs.cardinality() >= 2))
    {
      BitSet leftbs = (BitSet)this.argSets.get(0);
      BitSet rightbs = (BitSet)this.argSets.get(1);
      if ((leftbs.cardinality() == 1) || (rightbs.cardinality() == 1))
      {
        ValueExpr left = (ValueExpr)comparePredicate.args.get(0);
        ValueExpr right = (ValueExpr)comparePredicate.args.get(1);
        this.equijoin = new Condition.JoinExpr(this.predicate, bs, left, leftbs, right, rightbs, this.joinInfo);
      }
    }
    return null;
  }
}
