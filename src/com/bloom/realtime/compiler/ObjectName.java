package com.bloom.runtime.compiler;

import org.apache.commons.lang.builder.HashCodeBuilder;

public class ObjectName
{
  String namespace;
  String name;
  
  private ObjectName(String namespace, String name)
  {
    this.namespace = namespace;
    this.name = name;
  }
  
  public static ObjectName makeObjectName(String namespace, String name)
  {
    return new ObjectName(namespace, name);
  }
  
  public String getFullName()
  {
    return getNamespace() + "." + getName();
  }
  
  public String getName()
  {
    return this.name;
  }
  
  public String getNamespace()
  {
    return this.namespace;
  }
  
  public String toString()
  {
    return " Namespace : " + this.namespace + "Name : " + this.name;
  }
  
  public boolean equals(Object obj)
  {
    return (this.namespace == ((ObjectName)obj).namespace) && (this.name == ((ObjectName)obj).name);
  }
  
  public int hashCode()
  {
    return new HashCodeBuilder().append(this.namespace).append(this.name).toHashCode();
  }
}
