package com.bloom.runtime.meta;

import java.util.HashMap;
import java.util.LinkedHashSet;

public class Graph<UUID, Set>
  extends HashMap<UUID, Set>
{
  public Set get(Object key)
  {
    synchronized (key)
    {
      if (key == null) {
        return null;
      }
      if (super.get(key) == null) {
        super.put(key, new LinkedHashSet());
      }
      return (Set)super.get(key);
    }
  }
}
