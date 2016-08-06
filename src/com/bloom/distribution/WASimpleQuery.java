package com.bloom.distribution;

import java.util.Collection;
import java.util.List;

import com.bloom.waction.Waction;

public abstract interface WASimpleQuery<K, V>
{
  public abstract List<V> executeQuery(Queryable<K, V> paramQueryable);
  
  public abstract void addQueryResults(Iterable<Waction> paramIterable, Collection<V> paramCollection);
  
  public abstract List<V> getQueryResults();
  
  public abstract void mergeQueryResults(List<V> paramList);
}

