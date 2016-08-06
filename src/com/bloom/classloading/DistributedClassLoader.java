package com.bloom.classloading;

import com.bloom.metaRepository.HazelcastSingleton;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import javassist.ClassPool;
import org.apache.log4j.Logger;

public abstract class DistributedClassLoader
  extends ClassLoader
{
  static Logger logger = Logger.getLogger(DistributedClassLoader.class);
  final boolean isClient;
  ClassPool pool;
  String name;
  
  public DistributedClassLoader()
  {
    this(DistributedClassLoader.class.getClassLoader(), true, false);
  }
  
  public DistributedClassLoader(boolean isClient)
  {
    this(DistributedClassLoader.class.getClassLoader(), true, isClient);
  }
  
  public DistributedClassLoader(ClassLoader parent)
  {
    this(parent, true, false);
  }
  
  public DistributedClassLoader(ClassLoader parent, boolean isClient)
  {
    this(parent, true, isClient);
  }
  
  private DistributedClassLoader(ClassLoader parent, boolean isBridgeLoader, boolean isClient)
  {
    super(parent);
    this.isClient = isClient;
  }
  
  public Class<?> loadClass(String name)
    throws ClassNotFoundException
  {
    if (logger.isTraceEnabled()) {
      logger.trace(getName() + ": load class " + name);
    }
    return loadClass(name, false);
  }
  
  public ClassPool getPool()
  {
    return this.pool;
  }
  
  public String getName()
  {
    return this.name;
  }
  
  public void setName(String name)
  {
    this.name = name;
  }
  
  public BundleDefinition getBundleDefinition(String name)
  {
    return (BundleDefinition)getBundles().get(name);
  }
  
  public ClassDefinition getClassDefinition(String name)
  {
    return (ClassDefinition)getClasses().get(name);
  }
  
  public String getBundleNameForClass(String name)
  {
    return (String)getClassToBundleResolver().get(name);
  }
  
  protected static IMap<String, BundleDefinition> getBundles()
  {
    IMap<String, BundleDefinition> bundles = HazelcastSingleton.get().getMap("#bundles");
    return bundles;
  }
  
  public static IMap<String, ClassDefinition> getClasses()
  {
    IMap<String, ClassDefinition> classes = HazelcastSingleton.get().getMap("#classes");
    return classes;
  }
  
  protected static IMap<String, String> getClassToBundleResolver()
  {
    IMap<String, String> classToBundleResolver = HazelcastSingleton.get().getMap("#classResolver");
    return classToBundleResolver;
  }
}
