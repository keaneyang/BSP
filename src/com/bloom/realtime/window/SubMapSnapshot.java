package com.bloom.runtime.window;

import com.bloom.runtime.containers.WAEvent;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;

abstract class SubMapSnapshot
  extends RangeSnapshot
{
  private final ConcurrentNavigableMap<Long, BufWindow.Bucket> elements;
  private transient ConcurrentNavigableMap<Long, BufWindow.Bucket> range;
  
  private ConcurrentNavigableMap<Long, BufWindow.Bucket> getRange()
  {
    if (this.range == null) {
      this.range = this.elements.subMap(Long.valueOf(this.vHead & 0xFFFFFFFFFFFFFFC0), Long.valueOf(this.vTail));
    }
    return this.range;
  }
  
  SubMapSnapshot(long vHead, long vTail, ConcurrentNavigableMap<Long, BufWindow.Bucket> els)
  {
    super(vHead, vTail);
    this.elements = els;
  }
  
  public Iterator<BufWindow.Bucket> getBuffersIterator()
  {
    return getRange().values().iterator();
  }
  
  public WAEvent get(int index)
  {
    long idx = this.vHead + index;
    if (idx >= this.vTail) {
      throw new IndexOutOfBoundsException("Index: " + index);
    }
    BufWindow.Bucket bucket = (BufWindow.Bucket)getRange().get(Long.valueOf(idx & 0xFFFFFFFFFFFFFFC0));
    WEntry e = (WEntry)bucket.get((int)(idx & 0x3F));
    return e.data;
  }
}
