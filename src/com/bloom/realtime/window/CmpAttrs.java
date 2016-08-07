package com.bloom.runtime.window;

import com.bloom.runtime.containers.WAEvent;

public abstract interface CmpAttrs
{
  public abstract boolean inRange(WAEvent paramWAEvent1, WAEvent paramWAEvent2);
}
