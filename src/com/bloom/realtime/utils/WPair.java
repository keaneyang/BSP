package com.bloom.runtime.utils;

import java.io.Serializable;

public class WPair<A, B>
  implements Serializable
{
  private static final long serialVersionUID = -799825210823045963L;
  public A first;
  public B second;
  
  private WPair(A first, B second)
  {
    this.first = first;
    this.second = second;
  }
  
  public final boolean equals(Object o)
  {
    if (!(o instanceof WPair)) {
      return false;
    }
    WPair<A, B> other = (WPair)o;
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
  
  public static <A, B> WPair<A, B> make(A a, B b)
  {
    return new WPair(a, b);
  }
  
  public void setAll(A first, B second)
  {
    this.first = first;
    this.second = second;
  }
}
