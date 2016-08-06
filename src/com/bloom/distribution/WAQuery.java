package com.bloom.distribution;

import java.io.Serializable;
import java.util.Map;

public abstract interface WAQuery<K, V>
{
  public abstract boolean usesSingleKey();
  
  public abstract K getSingleKey();
  
  public abstract void setSingleKey(K paramK);
  
  public abstract boolean usesPartitionKey();
  
  public abstract Object getPartitionKey();
  
  public abstract ResultStats getResultStats(Queryable<K, V> paramQueryable);
  
  public abstract void run(Queryable<K, V> paramQueryable);
  
  public abstract void runOne(V paramV);
  
  public abstract Map<K, Map<String, Object>> getResults();
  
  public abstract void mergeResults(Map<K, Map<String, Object>> paramMap);
  
  public static class ResultStats
    implements Serializable
  {
    private static final long serialVersionUID = -798485429514059103L;
    public long startTime;
    public long endTime;
    public long count;
    
    public String toString()
    {
      return "{startTime:" + this.startTime + ", endTime:" + this.endTime + ", count:" + this.count + "}";
    }
  }
}
