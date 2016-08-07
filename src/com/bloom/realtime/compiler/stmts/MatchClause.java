package com.bloom.runtime.compiler.stmts;

import java.util.List;

import com.bloom.runtime.compiler.exprs.ValueExpr;
import com.bloom.runtime.compiler.patternmatch.PatternDefinition;
import com.bloom.runtime.compiler.patternmatch.PatternNode;

public class MatchClause
{
  public final PatternNode pattern;
  public final List<PatternDefinition> definitions;
  public final List<ValueExpr> partitionkey;
  
  public MatchClause(PatternNode pattern, List<PatternDefinition> definitions, List<ValueExpr> partitionkey)
  {
    this.pattern = pattern;
    this.definitions = definitions;
    this.partitionkey = partitionkey;
  }
  
  public String toString()
  {
    return "MATCH " + this.pattern + "\nDEFINE " + this.definitions + (this.partitionkey != null ? "\nPARTITION BY " + this.partitionkey : "");
  }
}
