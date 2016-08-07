package com.bloom.runtime.compiler.patternmatch;

import java.util.List;

public abstract class ValidatedPatternNode
{
  private final int nodeid;
  
  public ValidatedPatternNode(int id)
  {
    this.nodeid = id;
  }
  
  public int getID()
  {
    return this.nodeid;
  }
  
  public abstract void emit(MatchPatternGenerator paramMatchPatternGenerator);
  
  public abstract String toString();
  
  public static ValidatedPatternNode makeRestartAnchor(int nodeid, final int id)
  {
    new ValidatedPatternNode(nodeid)
    {
      public void emit(MatchPatternGenerator g)
      {
        g.emitAnchor(this, id);
      }
      
      public String toString()
      {
        return "#";
      }
    };
  }
  
  public static ValidatedPatternNode makePatternVariable(int nodeid, final PatternVariable var)
  {
    new ValidatedPatternNode(nodeid)
    {
      public void emit(MatchPatternGenerator g)
      {
        g.emitVariable(this, var);
      }
      
      public String toString()
      {
        return var.getName();
      }
    };
  }
  
  public static ValidatedPatternNode makePatternRepetition(int nodeid, final ValidatedPatternNode subnode, final PatternRepetition repetition)
  {
    new ValidatedPatternNode(nodeid)
    {
      public void emit(MatchPatternGenerator g)
      {
        g.emitRepetition(this, subnode, repetition);
      }
      
      public String toString()
      {
        return subnode + repetition.toString();
      }
    };
  }
  
  public static ValidatedPatternNode makeAlternation(int nodeid, final List<ValidatedPatternNode> subnodes)
  {
    new ValidatedPatternNode(nodeid)
    {
      public void emit(MatchPatternGenerator g)
      {
        g.emitAlternation(this, subnodes);
      }
      
      public String toString()
      {
        StringBuilder b = new StringBuilder();
        String sep = "";
        for (ValidatedPatternNode n : subnodes)
        {
          b.append(sep).append(n);
          sep = "|";
        }
        return b.toString();
      }
    };
  }
  
  public static ValidatedPatternNode makeSequence(int nodeid, final List<ValidatedPatternNode> subnodes)
  {
    new ValidatedPatternNode(nodeid)
    {
      public void emit(MatchPatternGenerator g)
      {
        g.emitSequence(this, subnodes);
      }
      
      public String toString()
      {
        StringBuilder b = new StringBuilder();
        String sep = "";
        for (ValidatedPatternNode n : subnodes)
        {
          b.append(sep).append(n);
          sep = " ";
        }
        return "(" + b.toString() + ")";
      }
    };
  }
}
