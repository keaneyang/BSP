package com.bloom.waction;

import com.bloom.uuid.UUID;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.bloom.event.ObjectMapperFactory;

import flexjson.JSON;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class WactionContext
  implements Map<String, Object>, Serializable
{
  private static final transient long serialVersionUID = 1397157221930042651L;
  @JSON(include=false)
  @JsonIgnore
  protected static transient ObjectMapper jsonMapper = ObjectMapperFactory.getInstance();
  @JSON(include=false)
  @JsonIgnore
  public final transient Map<String, Object> map = new HashMap();
  public UUID wactionID;
  
  protected Object getWrappedValue(boolean val)
  {
    return new Boolean(val);
  }
  
  protected Object getWrappedValue(char val)
  {
    return new Character(val);
  }
  
  protected Object getWrappedValue(short val)
  {
    return new Short(val);
  }
  
  protected Object getWrappedValue(int val)
  {
    return new Integer(val);
  }
  
  protected Object getWrappedValue(long val)
  {
    return new Long(val);
  }
  
  protected Object getWrappedValue(float val)
  {
    return new Float(val);
  }
  
  protected Object getWrappedValue(double val)
  {
    return new Double(val);
  }
  
  protected Object getWrappedValue(Object val)
  {
    return val;
  }
  
  protected String getWrappedValue(String val)
  {
    return new String(val);
  }
  
  public String toJSON()
  {
    try
    {
      return jsonMapper.writeValueAsString(this);
    }
    catch (Exception e) {}
    return "<Undeserializable>";
  }
  
  public int size()
  {
    return this.map.size();
  }
  
  @JSON(include=false)
  @JsonIgnore
  public boolean isEmpty()
  {
    return this.map.isEmpty();
  }
  
  public boolean containsKey(Object key)
  {
    return this.map.containsKey(key);
  }
  
  public boolean containsValue(Object value)
  {
    return this.map.containsValue(value);
  }
  
  public Object remove(Object key)
  {
    return this.map.remove(key);
  }
  
  public void putAll(Map<? extends String, ? extends Object> m)
  {
    this.map.putAll(m);
  }
  
  public void clear()
  {
    this.map.clear();
  }
  
  public Set<String> keySet()
  {
    return this.map.keySet();
  }
  
  public Collection<Object> values()
  {
    return this.map.values();
  }
  
  public Set<Map.Entry<String, Object>> entrySet()
  {
    return this.map.entrySet();
  }
  
  public Object get(Object key)
  {
    return this.map.get(key);
  }
  
  public Object put(String key, Object value)
  {
    return this.map.put(key, value);
  }
  
  public String toString()
  {
    return toJSON();
  }
}
