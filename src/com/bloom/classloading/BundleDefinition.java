package com.bloom.classloading;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class BundleDefinition
  implements Serializable
{
  private static final long serialVersionUID = 2761117186639790656L;
  private String jarPath;
  private String name;
  private String appName;
  private Type type;
  private List<String> classNames;
  
  public static enum Type
  {
    undefined,  jar,  type,  context,  query,  queryTask,  keyFactory,  fieldFactory,  waction,  recordConverter,  attrExtractor;
    
    private Type() {}
  }
  
  public BundleDefinition(Type type, String appName, String jarPath, String name)
  {
    this.type = type;
    this.appName = appName;
    this.jarPath = jarPath;
    this.name = name;
    this.classNames = new ArrayList();
  }
  
  public BundleDefinition(Type type, String appName, String name)
  {
    this(type, appName, null, name);
  }
  
  public BundleDefinition()
  {
    this(Type.undefined, null, null, null);
  }
  
  public void addClassName(String className)
  {
    if (this.classNames.contains(className)) {
      throw new IllegalArgumentException("Class " + className + " is already present in " + this.name);
    }
    this.classNames.add(className);
  }
  
  public String getJarPath()
  {
    return this.jarPath;
  }
  
  public void setJarPath(String jarPath)
  {
    this.jarPath = jarPath;
  }
  
  public String getName()
  {
    return this.name;
  }
  
  public void setName(String name)
  {
    this.name = name;
  }
  
  public String getAppName()
  {
    return this.appName;
  }
  
  public void setAppName(String appName)
  {
    this.appName = appName;
  }
  
  public Type getType()
  {
    return this.type;
  }
  
  public void setType(Type type)
  {
    this.type = type;
  }
  
  public List<String> getClassNames()
  {
    return this.classNames;
  }
  
  public void setClassNames(List<String> classNames)
  {
    this.classNames = classNames;
  }
  
  public static String getUri(String appName, Type type, String name)
  {
    return appName + ":" + type + ":" + name;
  }
  
  public String getUri()
  {
    return getUri(this.appName, this.type, this.name);
  }
}
