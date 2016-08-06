package com.bloom.wactionstore;

import java.util.Map;

import com.bloom.runtime.Property;

public enum Type
{
  IN_MEMORY,  STANDARD,  INTERVAL,  UNKNOWN;
  
  private static final String TYPE_NAME_PREFIX = "$Type";
  
  private Type() {}
  
  public String typeName()
  {
    return "$Type." + name();
  }
  
  public static Type getType(Map<String, Object> properties, Type defaultType)
  {
    if (properties != null) {
      for (String propertyName : properties.keySet())
      {
        Type result = getTypeFromString(propertyName);
        if (result != null) {
          return result;
        }
      }
    }
    return defaultType;
  }
  
  public static Type getType(Iterable<Property> properties, Type defaultType)
  {
    if (properties != null) {
      for (Property property : properties)
      {
        Type result = getTypeFromString(property.name);
        if (result != null) {
          return result;
        }
      }
    }
    return defaultType;
  }
  
  public static Type getTypeFromString(String string)
  {
    if (string.equalsIgnoreCase(STANDARD.typeName())) {
      return STANDARD;
    }
    if (string.equalsIgnoreCase(IN_MEMORY.typeName())) {
      return IN_MEMORY;
    }
    if (string.equalsIgnoreCase(INTERVAL.typeName())) {
      return INTERVAL;
    }
    return null;
  }
}
