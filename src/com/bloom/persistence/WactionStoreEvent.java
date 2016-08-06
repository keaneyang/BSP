package com.bloom.persistence;

import com.bloom.waction.Waction;

public class WactionStoreEvent
{
  private final Waction value;
  
  public WactionStoreEvent(Waction value)
  {
    this.value = value;
  }
  
  public Waction getValue()
  {
    return this.value;
  }
}
