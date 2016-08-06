package com.bloom.wactionstore.base;

import java.util.HashMap;
import java.util.Map;

import com.bloom.wactionstore.WActionStore;

public abstract class WActionStoreBase<T extends WActionStoreManagerBase>
  implements WActionStore
{
  private final T manager;
  private final String name;
  private final Map<String, Object> properties = new HashMap();
  
  protected WActionStoreBase(T manager, String name, Map<String, Object> properties)
  {
    this.manager = manager;
    this.name = name;
    if (properties != null) {
      this.properties.putAll(properties);
    }
  }
  
  public T getManager()
  {
    return this.manager;
  }
  
  public String getName()
  {
    return this.name;
  }
  
  public Map<String, Object> getProperties()
  {
    return this.properties;
  }
}
