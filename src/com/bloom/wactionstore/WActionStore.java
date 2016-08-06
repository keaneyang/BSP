package com.bloom.wactionstore;

import com.bloom.persistence.WactionStore;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Iterator;
import java.util.Map;

public abstract interface WActionStore
{
  public abstract WActionStoreManager getManager();
  
  public abstract String getName();
  
  public abstract Map<String, Object> getProperties();
  
  public abstract Iterator<String> getNames();
  
  public abstract DataType get(String paramString, Map<String, Object> paramMap);
  
  public abstract boolean remove(String paramString);
  
  public abstract DataType createDataType(String paramString, JsonNode paramJsonNode);
  
  public abstract DataType setDataType(String paramString, JsonNode paramJsonNode, WactionStore paramWactionStore);
  
  public abstract void flush();
  
  public abstract void fsync();
  
  public abstract long getWActionCount();
}
