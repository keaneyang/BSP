package com.bloom.runtime.compiler.stmts;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import com.bloom.runtime.Property;
import com.bloom.runtime.utils.Factory;

public class MappedStream
  implements Serializable
{
  public String streamName;
  public final Map<String, Object> mappingProperties;
  
  public MappedStream()
  {
    this.streamName = null;
    this.mappingProperties = null;
  }
  
  public MappedStream(String stream, List<Property> map_props)
  {
    this.streamName = stream;
    this.mappingProperties = combineProperties(map_props);
  }
  
  public String toString()
  {
    return this.streamName + " MAP(" + this.mappingProperties.toString() + ")";
  }
  
  private Map<String, Object> combineProperties(List<Property> propList)
  {
    Map<String, Object> props = Factory.makeCaseInsensitiveMap();
    if (propList != null) {
      for (Property p : propList) {
        if ((p.value instanceof List))
        {
          Object value = combineProperties((List)p.value);
          props.put(p.name, value);
        }
        else
        {
          props.put(p.name, p.value);
        }
      }
    }
    return props;
  }
}
