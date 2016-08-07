package com.bloom.runtime.window;

import java.lang.ref.ReferenceQueue;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListSet;

class SkipListSnapshotRefIndex
  implements SnapshotRefIndex
{
  public static final SnapshotRefIndexFactory factory = new SnapshotRefIndexFactory()
  {
    public SnapshotRefIndex create()
    {
      return new SkipListSnapshotRefIndex();
    }
  };
  private static final Comparator<CmpSnapshotRef> comparator = new Comparator()
  {
    public int compare(SkipListSnapshotRefIndex.CmpSnapshotRef o1, SkipListSnapshotRefIndex.CmpSnapshotRef o2)
    {
      if (o1.head < o2.head) {
        return -1;
      }
      if (o1.head > o2.head) {
        return 1;
      }
      if (o1.id < o2.id) {
        return -1;
      }
      if (o1.id > o2.id) {
        return 1;
      }
      return 0;
    }
  };
  private final ConcurrentSkipListSet<CmpSnapshotRef> index;
  
  SkipListSnapshotRefIndex()
  {
    this.index = new ConcurrentSkipListSet(comparator);
  }
  
  public void add(BufWindow owner, Snapshot sn, ReferenceQueue<? super Snapshot> q)
  {
    CmpSnapshotRef ref = new CmpSnapshotRef(sn.vHead, owner, sn, q);
    this.index.add(ref);
  }
  
  public long removeAndGetOldest(SnapshotRef ref)
  {
    this.index.remove(ref);
    try
    {
      return ((CmpSnapshotRef)this.index.first()).head;
    }
    catch (NoSuchElementException e) {}
    return -1L;
  }
  
  public List<? extends SnapshotRef> toList()
  {
    return new ArrayList(this.index);
  }
  
  private static class CmpSnapshotRef
    extends SnapshotRef
  {
    private static volatile int next_id = 0;
    private final int id;
    private final long head;
    
    CmpSnapshotRef(long head, BufWindow buffer, Snapshot referent, ReferenceQueue<? super Snapshot> q)
    {
      super(referent, q);
      this.head = head;
      this.id = (next_id++);
    }
    
    public String toString()
    {
      return "snapshotRef(" + this.head + "," + this.id + "=" + get() + ")";
    }
  }
  
  public boolean isEmpty()
  {
    return this.index.isEmpty();
  }
  
  public String toString()
  {
    return this.index.toString();
  }
  
  public int size()
  {
    return this.index.size();
  }
}
