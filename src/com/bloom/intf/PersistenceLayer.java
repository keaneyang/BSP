package com.bloom.intf;

import com.bloom.persistence.WactionStore;
import com.bloom.waction.Waction;
import com.bloom.waction.WactionKey;
import com.bloom.recovery.Position;

import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract interface PersistenceLayer
{
  public abstract void init();
  
  public abstract void init(String paramString);
  
  public abstract int delete(Object paramObject);
  
  public abstract Range[] persist(Object paramObject);
  
  public abstract void merge(Object paramObject);
  
  public abstract void setStoreName(String paramString1, String paramString2);
  
  public abstract Object get(Class<?> paramClass, Object paramObject);
  
  public abstract List<?> runQuery(String paramString, Map<String, Object> paramMap, Integer paramInteger);
  
  public abstract Object runNativeQuery(String paramString);
  
  public abstract int executeUpdate(String paramString, Map<String, Object> paramMap);
  
  public abstract void close();
  
  public abstract Position getWSPosition(String paramString1, String paramString2);
  
  public abstract boolean clearWSPosition(String paramString1, String paramString2);
  
  public abstract <T extends Waction> Iterable<T> getResults(Class<T> paramClass, Map<String, Object> paramMap, Set<WactionKey> paramSet);
  
  public abstract <T extends Waction> Iterable<T> getResults(Class<T> paramClass, String paramString, Map<String, Object> paramMap);
  
  public abstract WactionStore getWactionStore();
  
  public abstract void setWactionStore(WactionStore paramWactionStore);
  
  public static class Range
  {
    private final int start;
    private final int end;
    private boolean isSuccessful = false;
    
    public Range(int start, int end)
    {
      this.start = start;
      this.end = end;
    }
    
    public int getStart()
    {
      return this.start;
    }
    
    public int getEnd()
    {
      return this.end;
    }
    
    public boolean isSuccessful()
    {
      return this.isSuccessful;
    }
    
    public void setSuccessful(boolean isSuccessful)
    {
      this.isSuccessful = isSuccessful;
    }
  }
}
