package com.bloom.runtime;

import java.io.Serializable;
import org.apache.commons.lang.builder.HashCodeBuilder;

public class Property
  implements Serializable
{
  private static final long serialVersionUID = 6042801189452241946L;
  public final String name;
  public final Object value;
  
  public Property()
  {
    this.name = null;
    this.value = null;
  }
  
  public Property(String nameValue)
  {
    int colonPos = nameValue.indexOf(':');
    if (colonPos == -1) {
      throw new IllegalArgumentException("Invalid property " + nameValue + " should be of form name:Value");
    }
    this.name = nameValue.substring(0, colonPos);
    String tempValue = nameValue.substring(colonPos + 1).trim();
    char bChar = tempValue.charAt(0);
    char eChar = tempValue.charAt(tempValue.length() - 1);
    if (((bChar == '\'') && (eChar == '\'')) || ((bChar == '"') && (eChar == '"'))) {
      tempValue = tempValue.substring(1, tempValue.length() - 1);
    }
    this.value = tempValue;
  }
  
  public Property(String name, Object value)
  {
    this.name = name;
    this.value = value;
  }
  
  public String toString()
  {
    return "Prop(" + this.name + ":" + this.value + ")";
  }
  
  public boolean equals(Object obj)
  {
    Property prop = (Property)obj;
    return (prop.name == this.name) && (prop.value == this.value);
  }
  
  public int hashCode()
  {
    return new HashCodeBuilder().append(this.name).append(this.value).toHashCode();
  }
}
