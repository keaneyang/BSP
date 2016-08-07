package com.bloom.runtime.window;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;

public abstract class SnapshotRef
  extends WeakReference<Snapshot>
{
  final BufWindow buffer;
  
  SnapshotRef(BufWindow buffer, Snapshot referent, ReferenceQueue<? super Snapshot> q)
  {
    super(referent, q);
    this.buffer = buffer;
  }
  
  public abstract String toString();
}
