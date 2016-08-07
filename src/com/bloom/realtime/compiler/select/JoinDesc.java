package com.bloom.runtime.compiler.select;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import com.bloom.runtime.compiler.exprs.Expr;
import com.bloom.runtime.compiler.exprs.Predicate;

public class JoinDesc
{
  public final JoinAlgorithm algorithm;
  public final BitSet joinSet;
  public final DataSet withDataSet;
  public final List<Condition> afterJoinConditions;
  public final Condition joinCondition;
  public final IndexDesc joinIndex;
  public final List<Expr> searchExprs;
  public final List<Predicate> postFilters;
  public final int windowIndexID;
  public final boolean isOuter;
  
  public static enum JoinAlgorithm
  {
    SCAN,  INDEX_LOOKUP,  WINDOW_INDEX_LOOKUP,  WITH_STREAM;
    
    private JoinAlgorithm() {}
  }
  
  public JoinDesc(JoinAlgorithm algo, BitSet joinSet, DataSet dataset, Condition jc, IndexDesc id, List<Expr> searchExprs, List<Predicate> postFilters, int windowIndexID, boolean isOuter)
  {
    this.algorithm = algo;
    this.joinSet = ((BitSet)joinSet.clone());
    this.withDataSet = dataset;
    this.afterJoinConditions = new ArrayList(0);
    this.joinCondition = jc;
    this.joinIndex = id;
    this.searchExprs = searchExprs;
    this.postFilters = postFilters;
    this.windowIndexID = windowIndexID;
    this.isOuter = isOuter;
  }
  
  public String toString()
  {
    return "JoinDesc(" + this.algorithm + " set " + this.joinSet + (this.joinCondition == null ? " crossjoin " : " join ") + this.withDataSet.getName() + " {" + this.withDataSet.getID() + "}" + (this.joinCondition == null ? "" : new StringBuilder().append("\n\ton ").append(this.joinCondition).toString()) + (this.joinIndex == null ? "" : new StringBuilder().append("\n\tindex ").append(this.joinIndex).toString()) + (this.searchExprs == null ? "" : new StringBuilder().append("\n\tsearch ").append(this.searchExprs).toString()) + "\n\tpostFilters " + this.postFilters + "\n\twindowIndexID " + this.windowIndexID + "\n\tisOuter " + this.isOuter + "\n\tafter join filters " + this.afterJoinConditions + ")";
  }
  
  public Class<?>[] getSearchKeySignature()
  {
    return Expr.getSignature(this.searchExprs);
  }
}
