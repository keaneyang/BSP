package com.bloom.runtime.compiler.patternmatch;

import com.bloom.runtime.compiler.exprs.Predicate;
import com.bloom.runtime.compiler.select.DataSet;

public abstract class PatternVariable
{
  private final String name;
  private final int id;
  
  PatternVariable(int id, String name)
  {
    this.id = id;
    this.name = name;
  }
  
  public boolean equals(Object other)
  {
    if (this == other) {
      return true;
    }
    if (!(other instanceof PatternVariable)) {
      return false;
    }
    PatternVariable o = (PatternVariable)other;
    return this.id == o.id;
  }
  
  public int hashCode()
  {
    return this.id;
  }
  
  public int getId()
  {
    return this.id;
  }
  
  public String getName()
  {
    return this.name;
  }
  
  public boolean isTimer()
  {
    return false;
  }
  
  public Class<?> getJavaType()
  {
    return Object.class;
  }
  
  public void analyze(MatchValidator v) {}
  
  public DataSet getDataSet()
  {
    return null;
  }
  
  public abstract void validate(MatchValidator paramMatchValidator);
  
  public abstract void emit(MatchExprGenerator paramMatchExprGenerator);
  
  public abstract boolean isEventVar();
  
  public abstract String toString();
  
  public static PatternVariable makeCondition(final int id, String name, final Predicate pred)
  {
    new PatternVariable(id, name)
    {
      Predicate p;
      
      public void validate(MatchValidator v)
      {
        this.p = v.validateCondtion(pred);
      }
      
      public void emit(MatchExprGenerator g)
      {
        g.emitCondition(id, this.p);
      }
      
      public void analyze(MatchValidator v)
      {
        this.p = v.analyzePredicate(this.p);
      }
      
      public String toString()
      {
        return "(" + pred + ")";
      }
      
      public boolean isEventVar()
      {
        return false;
      }
    };
  }
  
  public static PatternVariable makeVar(final int id, final String name, final Predicate pred, String sname, final DataSet ds)
  {
    new PatternVariable(id, name)
    {
      Predicate p;
      
      public void validate(MatchValidator v)
      {
        this.p = v.validateVarPredicate(pred, ds);
      }
      
      public void emit(MatchExprGenerator g)
      {
        g.emitVariable(id, this.p, ds);
      }
      
      public void analyze(MatchValidator v)
      {
        this.p = v.analyzePredicate(this.p);
      }
      
      public Class<?> getJavaType()
      {
        return ds.getJavaType();
      }
      
      public String toString()
      {
        return name + "(" + pred + ")";
      }
      
      public boolean isEventVar()
      {
        return true;
      }
      
      public DataSet getDataSet()
      {
        return ds;
      }
    };
  }
  
  public static PatternVariable makeTimerFunc(final int id, final String name, final Predicate pred, String sname, TimerFunc f)
  {
    switch (f)
    {
    case CREATE: 
      new PatternVariable(id, name)
      {
        long i;
        
        public void validate(MatchValidator v)
        {
          this.i = v.validateCreateTimer(pred);
        }
        
        public void emit(MatchExprGenerator g)
        {
          g.emitCreateTimer(id, this.i);
        }
        
        public boolean isTimer()
        {
          return true;
        }
        
        public String toString()
        {
          return name + "->timer[" + id + "](" + this.i + ")";
        }
        
        public boolean isEventVar()
        {
          return false;
        }
      };
    case STOP: 
      new PatternVariable(id, name)
      {
        int timerid;
        
        public void validate(MatchValidator v)
        {
          this.timerid = v.validateStopTimer(pred);
        }
        
        public void emit(MatchExprGenerator g)
        {
          g.emitStopTimer(id, this.timerid);
        }
        
        public String toString()
        {
          return "stop(" + this.timerid + ")";
        }
        
        public boolean isEventVar()
        {
          return false;
        }
      };
    case WAIT: 
      new PatternVariable(id, name)
      {
        int timerid;
        
        public void validate(MatchValidator v)
        {
          this.timerid = v.validateWaitTimer(pred);
        }
        
        public void emit(MatchExprGenerator g)
        {
          g.emitWaitTimer(id, this.timerid);
        }
        
        public String toString()
        {
          return "wait(" + this.timerid + ")";
        }
        
        public boolean isEventVar()
        {
          return true;
        }
      };
    }
    throw new IllegalArgumentException();
  }
}
