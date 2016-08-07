package com.bloom.runtime.compiler.patternmatch;

import java.util.List;

public abstract interface MatchPatternGenerator
{
  public abstract void emitAnchor(ValidatedPatternNode paramValidatedPatternNode, int paramInt);
  
  public abstract void emitVariable(ValidatedPatternNode paramValidatedPatternNode, PatternVariable paramPatternVariable);
  
  public abstract void emitRepetition(ValidatedPatternNode paramValidatedPatternNode1, ValidatedPatternNode paramValidatedPatternNode2, PatternRepetition paramPatternRepetition);
  
  public abstract void emitAlternation(ValidatedPatternNode paramValidatedPatternNode, List<ValidatedPatternNode> paramList);
  
  public abstract void emitSequence(ValidatedPatternNode paramValidatedPatternNode, List<ValidatedPatternNode> paramList);
}

