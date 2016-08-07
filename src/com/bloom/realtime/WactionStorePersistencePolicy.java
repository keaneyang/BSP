package com.bloom.runtime;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

import com.bloom.wactionstore.Type;

public class WactionStorePersistencePolicy
{
  private static Logger logger = Logger.getLogger(WactionStorePersistencePolicy.class);
  public final Interval howOften;
  public final Type type;
  public final List<Property> properties = new ArrayList(1);
  
  public WactionStorePersistencePolicy(Interval howOften, List<Property> properties)
  {
    if ((howOften != null) && ((properties == null) || ((properties != null) && (properties.isEmpty())))) {
      throw new RuntimeException("Invalid Property set.");
    }
    Type wActionStoreType = Type.INTERVAL;
    this.howOften = howOften;
    if (properties != null) {
      for (Property property : properties)
      {
        String propertyName = property.name;
        if (propertyName.equalsIgnoreCase("storageProvider"))
        {
          String propertyValueString = (String)property.value;
          if ((propertyValueString.equalsIgnoreCase("elasticsearch")) || 
          
            (!propertyValueString.equalsIgnoreCase("jdbc"))) {
            wActionStoreType = Type.STANDARD;
          }
        }
        Type typeProperty = Type.getTypeFromString(propertyName);
        if (typeProperty != null) {
          wActionStoreType = typeProperty;
        } else {
          this.properties.add(property);
        }
      }
    }
    this.type = wActionStoreType;
    this.properties.add(new Property(this.type.typeName(), null));
  }
  
  public WactionStorePersistencePolicy(Type type, List<Property> properties)
  {
    this.howOften = null;
    if (properties != null) {
      for (Property property : properties)
      {
        Type typeProperty = Type.getTypeFromString(property.name);
        if (typeProperty == null) {
          this.properties.add(property);
        }
      }
    }
    this.type = type;
    this.properties.add(new Property(this.type.typeName(), null));
  }
  
  public WactionStorePersistencePolicy(List<Property> properties)
  {
    if ((properties == null) || ((properties != null) && (properties.isEmpty()))) {
      throw new RuntimeException("Invalid properties");
    }
    if (properties != null) {
      this.properties.addAll(properties);
    }
    this.type = getType(properties);
    if (this.type == Type.UNKNOWN) {
      throw new RuntimeException("provide persistence type.");
    }
    if (this.type == Type.INTERVAL) {
      this.howOften = getInterval(properties);
    } else {
      this.howOften = null;
    }
    this.properties.add(new Property(this.type.typeName(), null));
  }
  
  public Type getType(List<Property> properties)
  {
    Type type = Type.STANDARD;
    if (properties == null) {
      return type;
    }
    String key = "storageProvider";
    for (Property prop : properties) {
      if (prop.name.equalsIgnoreCase(key))
      {
        String val = (String)prop.value;
        if (val == null) {
          break;
        }
        if (val.equalsIgnoreCase("jdbc"))
        {
          type = Type.INTERVAL; break;
        }
        if (val.equalsIgnoreCase("elasticsearch"))
        {
          type = Type.STANDARD; break;
        }
        if (val.equalsIgnoreCase("inmemory"))
        {
          type = Type.IN_MEMORY; break;
        }
        type = Type.UNKNOWN; break;
      }
    }
    return type;
  }
  
  public Interval getInterval(List<Property> properties)
  {
    Interval interval = null;
    if (properties == null) {
      return interval;
    }
    String key = "persistence_interval";
    String val = null;
    for (Property prop : properties) {
      if (prop.name.equalsIgnoreCase(key))
      {
        val = (String)prop.value;
        break;
      }
    }
    if ((val == null) || (val.isEmpty())) {
      return interval;
    }
    val = val.trim().toLowerCase();
    if (!val.matches("(\\s*)([0-9]+)(\\s+)(sec.*|min.*)")) {
      return interval;
    }
    int time = Integer.parseInt(val.split("\\s+")[0]);
    if (val.split("\\s+")[1].startsWith("sec")) {
      interval = new Interval(1000000L * time);
    } else if (val.split("\\s+")[1].startsWith("min")) {
      interval = new Interval(60000000L * time);
    }
    return interval;
  }
}
