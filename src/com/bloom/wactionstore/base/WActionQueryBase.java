package com.bloom.wactionstore.base;

import com.bloom.wactionstore.WActionQuery;
import com.bloom.wactionstore.WActionStore;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class WActionQueryBase<T extends WActionStore>
  implements WActionQuery
{
  private final JsonNode queryJson;
  private final List<T> wActionStores;
  
  protected WActionQueryBase(JsonNode queryJson, Collection<T> wActionStores)
  {
    this.queryJson = queryJson;
    this.wActionStores = new ArrayList(wActionStores.size());
    this.wActionStores.addAll(wActionStores);
  }
  
  public JsonNode getQuery()
  {
    return this.queryJson;
  }
  
  public WActionStore getWActionStore(int index)
  {
	  WActionStore result = null;
    if ((index >= 0) && (index < this.wActionStores.size())) {
      result = (WActionStore)this.wActionStores.get(index);
    }
    return result;
  }
}
