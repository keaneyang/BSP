package com.bloom.runtime.compiler.stmts;

public class LimitClause
{
  public final int limit;
  public final int offset;
  
  public LimitClause(int limit, int offset)
  {
    this.limit = limit;
    this.offset = offset;
  }
}
