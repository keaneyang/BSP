package com.bloom.wactionstore;

import com.bloom.wactionstore.constants.Capability;
import com.bloom.wactionstore.constants.NameType;
import com.bloom.wactionstore.exceptions.WActionStoreException;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.Map;
import java.util.Set;

public abstract interface WActionStoreManager
{
  public abstract String getInstanceName();
  
  public abstract String getProviderName();
  
  public abstract Set<Capability> getCapabilities();
  
  public abstract CheckpointManager getCheckpointManager();
  
  public abstract String translateName(NameType paramNameType, String paramString);
  
  public abstract String[] getNames();
  
  public abstract WActionStore get(String paramString, Map<String, Object> paramMap);
  
  public abstract WActionStore getUsingAlias(String paramString, Map<String, Object> paramMap);
  
  public abstract boolean remove(String paramString);
  
  public abstract boolean removeUsingAlias(String paramString);
  
  public abstract WActionStore create(String paramString, Map<String, Object> paramMap);
  
  public abstract WActionStore getOrCreate(String paramString, Map<String, Object> paramMap);
  
  public abstract WActionQuery prepareQuery(JsonNode paramJsonNode)
    throws WActionStoreException;
  
  public abstract long delete(JsonNode paramJsonNode)
    throws WActionStoreException;
  
  public abstract void flush();
  
  public abstract void fsync();
  
  public abstract void close();
  
  public abstract void shutdown();
}

