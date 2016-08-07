package com.bloom.runtime.compiler.stmts;

import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.runtime.Interval;
import com.bloom.runtime.compiler.Compiler;
import com.bloom.runtime.components.EntityType;

import java.util.List;

public class CreateSorterStmt
  extends CreateStmt
{
  public final Interval sortTimeInterval;
  public final List<SorterInOutRule> inOutRules;
  public final String errorStream;
  
  public CreateSorterStmt(String name, Boolean doReplace, Interval sortInterval, List<SorterInOutRule> inOutRules, String errorStream)
  {
    super(EntityType.SORTER, name, doReplace.booleanValue());
    this.sortTimeInterval = sortInterval;
    this.inOutRules = inOutRules;
    this.errorStream = errorStream;
  }
  
  public String toString()
  {
    return stmtToString() + " OVER " + this.inOutRules + " WITHIN " + this.sortTimeInterval + " OUPUT ERRORS TO " + this.errorStream;
  }
  
  public Object compile(Compiler c)
    throws MetaDataRepositoryException
  {
    return c.compileCreateSorterStmt(this);
  }
}
