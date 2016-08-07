package com.bloom.runtime.compiler.select;

import java.util.List;

import com.bloom.runtime.compiler.exprs.Expr;

public class IndexDesc
{
  public final int id;
  public final DataSet dataset;
  public final List<Expr> exprs;
  
  public IndexDesc(int id, DataSet dataset, List<Expr> exprs)
  {
    this.id = id;
    this.dataset = dataset;
    this.exprs = exprs;
  }
  
  public Class<?>[] getIndexKeySignature()
  {
    return Expr.getSignature(this.exprs);
  }
  
  public String toString()
  {
    return "IndexDesc(" + this.id + " " + this.dataset.getName() + " " + this.exprs + ")";
  }
}
