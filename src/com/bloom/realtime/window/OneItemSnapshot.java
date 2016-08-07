package com.bloom.runtime.window;

import com.bloom.runtime.containers.WAEvent;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

class OneItemSnapshot
  extends Snapshot
{
  private final WEntry element;
  
  OneItemSnapshot(WEntry e)
  {
    super(e.id);
    this.element = e;
  }
  
  public Iterator<WEntry> itemIterator()
  {
    return Collections.singleton(this.element).iterator();
  }
  
  public int size()
  {
    return 1;
  }
  
  public Iterator<WAEvent> iterator()
  {
    return Collections.singleton(this.element.data).iterator();
  }
  
  public WAEvent get(int index)
  {
    if (index > 0) {
      throw new IndexOutOfBoundsException("Index: " + index);
    }
    return this.element.data;
  }
  
  public String toString()
  {
    return "{one item " + this.element + "}";
  }
}
