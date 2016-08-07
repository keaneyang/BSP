package com.bloom.runtime.window;

import java.lang.ref.ReferenceQueue;
import java.util.List;

public abstract interface SnapshotRefIndex
{
  public abstract void add(BufWindow paramBufWindow, Snapshot paramSnapshot, ReferenceQueue<? super Snapshot> paramReferenceQueue);
  
  public abstract long removeAndGetOldest(SnapshotRef paramSnapshotRef);
  
  public abstract List<? extends SnapshotRef> toList();
  
  public abstract boolean isEmpty();
  
  public abstract int size();
}
