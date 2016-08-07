package com.bloom.runtime.window;

import com.bloom.runtime.containers.WAEvent;
import java.util.Collections;
import java.util.Iterator;

class EmptySnapshot
  extends Snapshot
{
  EmptySnapshot(long head)
  {
    super(head);
  }
  
  public Iterator<WEntry> itemIterator()
  {
    return Collections.emptyIterator();
  }
  
  public int size()
  {
    return 0;
  }
  
  public WAEvent get(int index)
  {
    throw new IndexOutOfBoundsException("Index: " + index);
  }
  
  public Iterator<WAEvent> iterator()
  {
    return Collections.emptyIterator();
  }
  
  public String toString()
  {
    return "{empty snapshot}";
  }
}
