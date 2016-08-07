package com.bloom.runtime.window;

import com.bloom.runtime.containers.WAEvent;
import java.util.Iterator;

abstract class RangeSnapshot
  extends Snapshot
{
  protected final long vTail;
  
  private class DataIterator
    implements Iterator<WAEvent>
  {
    private final Iterator<WEntry> it;
    
    DataIterator(Iterator<WEntry> it)
    {
      this.it = it;
    }
    
    public boolean hasNext()
    {
      return this.it.hasNext();
    }
    
    public WAEvent next()
    {
      WEntry e = (WEntry)this.it.next();
      WAEvent o = e.data;
      return o;
    }
    
    public void remove()
    {
      this.it.remove();
    }
  }
  
  RangeSnapshot(long vHead, long vTail)
  {
    super(vHead);
    this.vTail = vTail;
    assert (vHead <= vTail);
  }
  
  abstract Iterator<BufWindow.Bucket> getBuffersIterator();
  
  private class WEntryIterator
    implements Iterator<WEntry>
  {
    private final Iterator<BufWindow.Bucket> it;
    private long idx;
    private BufWindow.Bucket bucket;
    
    WEntryIterator(Iterator<BufWindow.Bucket> it)
    {
      this.it = it;
      this.idx = RangeSnapshot.this.vHead;
    }
    
    public boolean hasNext()
    {
      return this.idx < RangeSnapshot.this.vTail;
    }
    
    public WEntry next()
    {
      assert (this.idx < RangeSnapshot.this.vTail);
      if (((this.idx & 0x3F) == 0L) || (this.idx == RangeSnapshot.this.vHead)) {
        this.bucket = ((BufWindow.Bucket)this.it.next());
      }
      WEntry e = (WEntry)this.bucket.get((int)(this.idx & 0x3F));
      if (e == null) {
        throw new RuntimeException("NULL WEntry in " + RangeSnapshot.this.getClass().getName() + " snapshot " + " (" + RangeSnapshot.this.vHead + "-" + RangeSnapshot.this.vTail + "=" + (RangeSnapshot.this.vTail - RangeSnapshot.this.vHead) + ")" + " index = " + this.idx + " bucket " + this.bucket);
      }
      this.idx += 1L;
      return e;
    }
    
    public void remove() {}
  }
  
  public Iterator<WEntry> itemIterator()
  {
    return new WEntryIterator(getBuffersIterator());
  }
  
  public int size()
  {
    return (int)(this.vTail - this.vHead);
  }
  
  public Iterator<WAEvent> iterator()
  {
    return new DataIterator(itemIterator());
  }
  
  public abstract WAEvent get(int paramInt);
  
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    Iterator<WAEvent> it = iterator();
    String sep = "";
    while (it.hasNext())
    {
      sb.append(sep + it.next());
      sep = ",";
    }
    sb.append("]");
    return "{vHead=" + this.vHead + " vTail=" + this.vTail + " els=" + sb + "}";
  }
}
