package com.bloom.distribution;

import java.util.Map;
import java.util.Set;

public abstract interface Queryable<K, V>
{
  public abstract Map<K, Map<String, Object>> query(WAQuery<K, V> paramWAQuery);
  
  public abstract void simpleQuery(WASimpleQuery<K, V> paramWASimpleQuery);
  
  public abstract WAQuery.ResultStats queryStats(WAQuery<K, V> paramWAQuery);
  
  public abstract <I> Map<K, V> getIndexedRange(String paramString, I paramI1, I paramI2);
  
  public abstract <I> Map<K, V> getIndexedEqual(String paramString, I paramI);
  
  public abstract V get(K paramK);
  
  public abstract Set<K> localKeys();
  
  public abstract Map<K, V> localEntries();
}

