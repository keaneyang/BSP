package com.bloom.runtime.compiler.select;

public class Var
{
  private final String prefix;
  private final int id;
  private final Class<?> type;
  private final boolean isStatic;
  
  public Var(String prefix, int id, Class<?> type, boolean isStatic)
  {
    assert (type != null);
    this.prefix = prefix;
    this.id = id;
    this.type = type;
    this.isStatic = isStatic;
  }
  
  public Class<?> getType()
  {
    return this.type;
  }
  
  public String getName()
  {
    return this.prefix + this.id;
  }
  
  public int getID()
  {
    return this.id;
  }
  
  public boolean isStatic()
  {
    return this.isStatic;
  }
  
  public int hashCode()
  {
    return this.isStatic ? -this.id : this.id;
  }
  
  public boolean equals(Object o)
  {
    if (!(o instanceof Var)) {
      return false;
    }
    Var v = (Var)o;
    return (this.id == v.id) && (this.isStatic == v.isStatic);
  }
  
  public String toString()
  {
    return getName();
  }
}
