package com.bloom.runtime.compiler.patternmatch;

import com.bloom.runtime.compiler.exprs.Predicate;
import com.bloom.runtime.compiler.select.DataSet;

public abstract interface MatchExprGenerator
{
  public abstract void emitCondition(int paramInt, Predicate paramPredicate);
  
  public abstract void emitVariable(int paramInt, Predicate paramPredicate, DataSet paramDataSet);
  
  public abstract void emitCreateTimer(int paramInt, long paramLong);
  
  public abstract void emitStopTimer(int paramInt1, int paramInt2);
  
  public abstract void emitWaitTimer(int paramInt1, int paramInt2);
}

