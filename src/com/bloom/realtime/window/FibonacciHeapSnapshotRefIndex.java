package com.bloom.runtime.window;

import java.lang.ref.ReferenceQueue;

class FibonacciHeapSnapshotRefIndex
  implements SnapshotRefIndex
{
  public static final SnapshotRefIndexFactory factory = new SnapshotRefIndexFactory()
  {
    public SnapshotRefIndex create()
    {
      return new FibonacciHeapSnapshotRefIndex();
    }
  };
  private final FibonacciHeap index;
  
  FibonacciHeapSnapshotRefIndex()
  {
    this.index = new FibonacciHeap();
  }
  
  public void add(BufWindow owner, Snapshot sn, ReferenceQueue<? super Snapshot> q)
  {
    NodeSnapshotRef ref = new NodeSnapshotRef(sn.vHead, owner, sn, q);
    synchronized (this.index)
    {
      ref.tag = this.index.insert(ref, sn.vHead);
    }
  }
  
  public long removeAndGetOldest(SnapshotRef ref)
  {
    assert ((ref instanceof NodeSnapshotRef));
    NodeSnapshotRef nref = (NodeSnapshotRef)ref;
    synchronized (this.index)
    {
      this.index.decreaseKey(nref.tag, -1L);
      FibonacciHeap.Node r = this.index.removeMin();
      assert (r == nref.tag);
      FibonacciHeap.Node n = this.index.min();
      return n != null ? n.getKey() : -1L;
    }
  }
  
  /* Error */
  public java.util.List<? extends SnapshotRef> toList()
  {
    // Byte code:
    //   0: aload_0
    //   1: getfield 4	com/bloom/runtime/window/FibonacciHeapSnapshotRefIndex:index	Lcom/bloom/runtime/window/FibonacciHeap;
    //   4: dup
    //   5: astore_1
    //   6: monitorenter
    //   7: aload_0
    //   8: getfield 4	com/bloom/runtime/window/FibonacciHeapSnapshotRefIndex:index	Lcom/bloom/runtime/window/FibonacciHeap;
    //   11: invokevirtual 20	com/bloom/runtime/window/FibonacciHeap:toList	()Ljava/util/List;
    //   14: aload_1
    //   15: monitorexit
    //   16: areturn
    //   17: astore_2
    //   18: aload_1
    //   19: monitorexit
    //   20: aload_2
    //   21: athrow
    // Line number table:
    //   Java source line #148	-> byte code offset #0
    //   Java source line #149	-> byte code offset #7
    //   Java source line #150	-> byte code offset #17
    // Local variable table:
    //   start	length	slot	name	signature
    //   0	22	0	this	FibonacciHeapSnapshotRefIndex
    //   5	14	1	Ljava/lang/Object;	Object
    //   17	4	2	localObject1	Object
    // Exception table:
    //   from	to	target	type
    //   7	16	17	finally
    //   17	20	17	finally
  }
  
  /* Error */
  public boolean isEmpty()
  {
    // Byte code:
    //   0: aload_0
    //   1: getfield 4	com/bloom/runtime/window/FibonacciHeapSnapshotRefIndex:index	Lcom/bloom/runtime/window/FibonacciHeap;
    //   4: dup
    //   5: astore_1
    //   6: monitorenter
    //   7: aload_0
    //   8: getfield 4	com/bloom/runtime/window/FibonacciHeapSnapshotRefIndex:index	Lcom/bloom/runtime/window/FibonacciHeap;
    //   11: invokevirtual 21	com/bloom/runtime/window/FibonacciHeap:isEmpty	()Z
    //   14: aload_1
    //   15: monitorexit
    //   16: ireturn
    //   17: astore_2
    //   18: aload_1
    //   19: monitorexit
    //   20: aload_2
    //   21: athrow
    // Line number table:
    //   Java source line #172	-> byte code offset #0
    //   Java source line #173	-> byte code offset #7
    //   Java source line #174	-> byte code offset #17
    // Local variable table:
    //   start	length	slot	name	signature
    //   0	22	0	this	FibonacciHeapSnapshotRefIndex
    //   5	14	1	Ljava/lang/Object;	Object
    //   17	4	2	localObject1	Object
    // Exception table:
    //   from	to	target	type
    //   7	16	17	finally
    //   17	20	17	finally
  }
  
  /* Error */
  public String toString()
  {
    // Byte code:
    //   0: aload_0
    //   1: getfield 4	com/bloom/runtime/window/FibonacciHeapSnapshotRefIndex:index	Lcom/bloom/runtime/window/FibonacciHeap;
    //   4: dup
    //   5: astore_1
    //   6: monitorenter
    //   7: aload_0
    //   8: getfield 4	com/bloom/runtime/window/FibonacciHeapSnapshotRefIndex:index	Lcom/bloom/runtime/window/FibonacciHeap;
    //   11: invokevirtual 22	com/bloom/runtime/window/FibonacciHeap:toString	()Ljava/lang/String;
    //   14: aload_1
    //   15: monitorexit
    //   16: areturn
    //   17: astore_2
    //   18: aload_1
    //   19: monitorexit
    //   20: aload_2
    //   21: athrow
    // Line number table:
    //   Java source line #179	-> byte code offset #0
    //   Java source line #180	-> byte code offset #7
    //   Java source line #181	-> byte code offset #17
    // Local variable table:
    //   start	length	slot	name	signature
    //   0	22	0	this	FibonacciHeapSnapshotRefIndex
    //   5	14	1	Ljava/lang/Object;	Object
    //   17	4	2	localObject1	Object
    // Exception table:
    //   from	to	target	type
    //   7	16	17	finally
    //   17	20	17	finally
  }
  
  private static class NodeSnapshotRef
    extends SnapshotRef
  {
    private FibonacciHeap.Node tag;
    
    NodeSnapshotRef(long head, BufWindow buffer, Snapshot referent, ReferenceQueue<? super Snapshot> q)
    {
      super(referent, q);
    }
    
    public String toString()
    {
      return "snapshotRef(" + this.tag.getKey() + "==" + get() + ")";
    }
  }
  
  public int size()
  {
    return this.index.size();
  }
}
