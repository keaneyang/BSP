package com.bloom.runtime.compiler.patternmatch;

import com.bloom.runtime.compiler.exprs.Predicate;

public class PatternDefinition
{
  public final String varName;
  public final String streamName;
  public final Predicate predicate;
  
  public PatternDefinition(String var, String streamName, Predicate p)
  {
    this.varName = var;
    this.streamName = streamName;
    this.predicate = p;
  }
  
  public String toString()
  {
    return this.varName + "=" + (this.streamName == null ? "" : this.streamName) + "(" + (this.predicate == null ? "" : this.predicate) + ")";
  }
}
