package com.bloom.wactionstore;

import com.bloom.wactionstore.exceptions.CapabilityException;
import com.fasterxml.jackson.databind.JsonNode;

public abstract interface WActionQuery
{
  public abstract JsonNode getQuery();
  
  public abstract WActionStore getWActionStore(int paramInt);
  
  public abstract Iterable<WAction> execute()
    throws CapabilityException;
}

