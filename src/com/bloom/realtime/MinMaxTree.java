package com.bloom.runtime;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.log4j.Logger;

class MinMaxTree<K>
  extends TreeMap<K, Long>
{
  private static Logger logger = Logger.getLogger(MinMaxTree.class);
  private static final long serialVersionUID = 2045578403717126481L;
  
  public void addKey(K key)
  {
    if (key == null) {
      return;
    }
    Long n = (Long)get(key);
    if (n == null) {
      put(key, Long.valueOf(1L));
    } else {
      put(key, Long.valueOf(n.longValue() + 1L));
    }
  }
  
  public void removeKey(K key)
  {
    if (key == null) {
      return;
    }
    Long n = (Long)get(key);
    if (n != null)
    {
      if (n.longValue() == 1L) {
        remove(key);
      } else {
        put(key, Long.valueOf(n.longValue() - 1L));
      }
    }
    else {
      logger.warn("remove key from MinMaxTree which has not been added");
    }
  }
  
  public K getMin()
  {
    if (size() > 0) {
      return (K)firstKey();
    }
    return null;
  }
  
  public K getMax()
  {
    if (size() > 0) {
      return (K)lastKey();
    }
    return null;
  }
  
  public K getMaxOccurs()
  {
    if (size() == 0) {
      return null;
    }
    long maxCount = 0L;
    K maxKey = null;
    for (Map.Entry<K, Long> entry : entrySet()) {
      if (((Long)entry.getValue()).longValue() > maxCount)
      {
        maxCount = ((Long)entry.getValue()).longValue();
        maxKey = entry.getKey();
      }
    }
    return maxKey;
  }
}
