package com.bloom.wactionstore;

import com.bloom.persistence.WactionStore;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jsonschema.main.JsonSchema;

import java.util.List;

public abstract interface DataType
{
  public abstract WActionStore getWActionStore();
  
  public abstract String getName();
  
  public abstract JsonNode getSchemaJson();
  
  public abstract JsonSchema getDataSchema();
  
  public abstract boolean isValid();
  
  public abstract boolean isValid(WAction paramWAction);
  
  public abstract void queue(WAction paramWAction);
  
  public abstract void queue(Iterable<WAction> paramIterable);
  
  public abstract boolean insert(WAction paramWAction);
  
  public abstract boolean insert(List<WAction> paramList, WactionStore paramWactionStore);
  
  public abstract void flush();
  
  public abstract void fsync();
}

