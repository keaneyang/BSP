package com.bloom.runtime.window;

import com.bloom.runtime.containers.WAEvent;
import java.util.AbstractList;
import java.util.Iterator;

public abstract class Snapshot
  extends AbstractList<WAEvent>
{
  final long vHead;
  
  public Snapshot(long head)
  {
    this.vHead = head;
  }
  
  public abstract Iterator<WEntry> itemIterator();
  
  public abstract Iterator<WAEvent> iterator();
}
