package com.bloom.runtime.window;

import java.util.concurrent.ConcurrentNavigableMap;

class MapRangeSnapshot
  extends SubMapSnapshot
{
  MapRangeSnapshot(long vHead, long vTail, ConcurrentNavigableMap<Long, BufWindow.Bucket> els)
  {
    super(vHead, vTail, els);
  }
}
