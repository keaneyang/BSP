package com.bloom.runtime.compiler.patternmatch;

import com.bloom.runtime.compiler.AST;
import com.bloom.runtime.utils.StringUtils;
import java.util.List;

public abstract class PatternNode
{
  public List<PatternNode> getElementsIfType(ComplexNodeType atype)
  {
    return null;
  }
  
  public abstract ValidatedPatternNode validate(MatchValidator paramMatchValidator);
  
  public abstract String toString();
  
  public static PatternNode makeRestartAnchor()
  {
    new PatternNode()
    {
      public ValidatedPatternNode validate(MatchValidator v)
      {
        return v.visitRestartAnchor();
      }
      
      public String toString()
      {
        return "#";
      }
    };
  }
  
  public static PatternNode makeRepetition(PatternNode element, final PatternRepetition reps)
  {
    if (reps == null) {
      return element;
    }
    new PatternNode()
    {
      public ValidatedPatternNode validate(MatchValidator v)
      {
        return v.visitRepetition(this.val$element, reps);
      }
      
      public String toString()
      {
        return this.val$element + "" + reps;
      }
    };
  }
  
  public static PatternNode makeVariable(String name)
  {
    new PatternNode()
    {
      public ValidatedPatternNode validate(MatchValidator v)
      {
        return v.visitVariable(this.val$name);
      }
      
      public String toString()
      {
        return this.val$name;
      }
    };
  }
  
  private static String getDelimiter(ComplexNodeType type)
  {
    switch (type)
    {
    case ALTERNATION: 
      return "|";
    case COMBINATION: 
      return "&";
    case SEQUENCE: 
      return " ";
    }
    return "";
  }
  
  private static PatternNode makeComplexNode(ComplexNodeType type, final List<PatternNode> elements)
  {
    new PatternNode()
    {
      public ValidatedPatternNode validate(MatchValidator v)
      {
        return v.visitComplexNode(this.val$type, elements);
      }
      
      public List<PatternNode> getElementsIfType(ComplexNodeType atype)
      {
        return atype == this.val$type ? elements : null;
      }
      
      public String toString()
      {
        String delim = PatternNode.getDelimiter(this.val$type);
        return "(" + StringUtils.join(elements, delim) + ")";
      }
    };
  }
  
  public static PatternNode makeComplexNode(ComplexNodeType type, PatternNode a, PatternNode b)
  {
    List<PatternNode> elements = a.getElementsIfType(type);
    if (elements != null) {
      return makeComplexNode(type, AST.NewList(elements, b));
    }
    return makeComplexNode(type, AST.NewList(a, b));
  }
}
