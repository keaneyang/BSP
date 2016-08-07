package com.bloom.runtime.window;

import com.bloom.runtime.meta.IntervalPolicy;
import com.bloom.runtime.meta.IntervalPolicy.Kind;

public class WindowPolicy
{
  private final Integer rowCount;
  private final CmpAttrs attrComparator;
  private final Long timeInterval;
  private final boolean isJumping;
  private final IntervalPolicy.Kind kind;
  private final SlidingPolicy slidingPolicy;
  
  private WindowPolicy(IntervalPolicy.Kind kind, boolean isJumping, Integer rowCount, CmpAttrs attrComparator, Long timeInterval, SlidingPolicy slidingPolicy)
  {
    this.kind = kind;
    this.isJumping = isJumping;
    this.rowCount = rowCount;
    this.attrComparator = attrComparator;
    this.timeInterval = timeInterval;
    this.slidingPolicy = slidingPolicy;
  }
  
  public Integer getRowCount()
  {
    return this.rowCount;
  }
  
  public CmpAttrs getComparator()
  {
    return this.attrComparator;
  }
  
  public Long getTimeInterval()
  {
    return this.timeInterval;
  }
  
  public boolean isJumping()
  {
    return this.isJumping;
  }
  
  public IntervalPolicy.Kind getKind()
  {
    return this.kind;
  }
  
  public SlidingPolicy getSlidingPolicy()
  {
    return this.slidingPolicy;
  }
  
  public String toString()
  {
    StringBuffer sb = new StringBuffer();
    sb.append(this.kind + " " + (this.isJumping ? "jumping" : "") + " ");
    if (this.rowCount != null) {
      sb.append("rowCount:" + this.rowCount + " ");
    }
    if (this.timeInterval != null) {
      sb.append("timeInterval:" + this.timeInterval + " ");
    }
    if (this.attrComparator != null) {
      sb.append("comparator:" + this.attrComparator + " ");
    }
    return sb.toString();
  }
  
  public static WindowPolicy makePolicy(IntervalPolicy.Kind kind, boolean isJumping, Integer rowCount, CmpAttrs attrComparator, Long timeInterval, SlidingPolicy slidingPolicy)
  {
    return new WindowPolicy(kind, isJumping, rowCount, attrComparator, timeInterval, slidingPolicy);
  }
}
