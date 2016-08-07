package com.bloom.runtime.window;

import com.bloom.runtime.containers.WAEvent;
import java.util.Iterator;
import java.util.List;

class BuffersListSnapshot
  extends RangeSnapshot
{
  private final List<BufWindow.Bucket> buffers;
  
  BuffersListSnapshot(long vHead, long vTail, List<BufWindow.Bucket> buffers)
  {
    super(vHead, vTail);
    this.buffers = buffers;
  }
  
  Iterator<BufWindow.Bucket> getBuffersIterator()
  {
    return this.buffers.iterator();
  }
  
  public WAEvent get(int index)
  {
    if (this.vHead + index >= this.vTail) {
      throw new IndexOutOfBoundsException("Index: " + index);
    }
    long idx = (this.vHead & 0x3F) + index;
    BufWindow.Bucket bucket = (BufWindow.Bucket)this.buffers.get((int)(idx / -64L));
    WEntry e = (WEntry)bucket.get((int)(idx & 0x3F));
    return e.data;
  }
}
