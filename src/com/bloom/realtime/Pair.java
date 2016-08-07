package com.bloom.runtime;

import java.io.Serializable;

public class Pair<A, B>
  implements Serializable
{
  private static final long serialVersionUID = -7998252108293045963L;
  public A first;
  public B second;
  
  public Pair()
  {
    this.first = null;
    this.second = null;
  }
  
  public Pair(A first, B second)
  {
    this.first = first;
    this.second = second;
  }
  
  public final boolean equals(Object o)
  {
    if (!(o instanceof Pair)) {
      return false;
    }
    Pair<A, B> other = (Pair)o;
    return (this.first == null ? other.first == null : this.first.equals(other.first)) && (this.second == null ? other.second == null : this.second.equals(other.second));
  }
  
  public final int hashCode()
  {
    return (this.first == null ? 0 : this.first.hashCode()) ^ (this.second == null ? 0 : this.second.hashCode());
  }
  
  public final String toString()
  {
    return "(" + this.first + "," + this.second + ")";
  }
  
  public static <A, B> Pair<A, B> make(A a, B b)
  {
    return new Pair(a, b);
  }
}
