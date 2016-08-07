package com.bloom.runtime.components;

public class Link
{
  public final Subscriber subscriber;
  public final int linkID;
  
  public Link(Subscriber sub, int linkID)
  {
    this.subscriber = sub;
    this.linkID = linkID;
  }
  
  public Link(Subscriber sub)
  {
    this(sub, -1);
  }
  
  public boolean equals(Object obj)
  {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof Link)) {
      return false;
    }
    Link l = (Link)obj;
    return (this.subscriber.equals(l.subscriber)) && (this.linkID == l.linkID);
  }
  
  public final int hashCode()
  {
    return this.subscriber.hashCode() ^ this.linkID;
  }
  
  public String toString()
  {
    return this.subscriber + " over " + this.linkID;
  }
}
