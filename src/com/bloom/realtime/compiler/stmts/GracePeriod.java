package com.bloom.runtime.compiler.stmts;

import java.io.Serializable;

import com.bloom.runtime.Interval;

public class GracePeriod
  implements Serializable
{
  private static final long serialVersionUID = 6064432984395345052L;
  public final Interval sortTimeInterval;
  public final String fieldName;
  
  public GracePeriod(Interval sortInterval, String fieldName)
  {
    this.sortTimeInterval = sortInterval;
    this.fieldName = fieldName;
  }
}
