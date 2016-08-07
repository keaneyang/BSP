package com.bloom.runtime.window;

import java.util.concurrent.ConcurrentSkipListMap;

class OverlappingSnapshot
  extends SubMapSnapshot
{
  final MapRangeSnapshot originalSnapshot;
  
  OverlappingSnapshot(MapRangeSnapshot originalSnapshot, long vTail, ConcurrentSkipListMap<Long, BufWindow.Bucket> elements)
  {
    super(originalSnapshot.vHead, vTail, elements);
    assert (originalSnapshot.vHead <= vTail);
    this.originalSnapshot = originalSnapshot;
  }
  
  public Snapshot getOriginalSnapshot()
  {
    return this.originalSnapshot;
  }
}
