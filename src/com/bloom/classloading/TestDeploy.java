package com.bloom.classloading;

import com.hazelcast.core.IMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.log4j.Logger;

public class TestDeploy
{
  private static Logger logger = Logger.getLogger(TestDeploy.class);
  
  public static void outputBundles(WALoader bl)
  {
    for (String bundleName : DistributedClassLoader.getBundles().keySet())
    {
      if (logger.isDebugEnabled()) {
        logger.debug("Bundle: " + bundleName);
      }
      BundleDefinition def = (BundleDefinition)DistributedClassLoader.getBundles().get(bundleName);
      for (String className : def.getClassNames())
      {
        if (logger.isDebugEnabled()) {
          logger.debug("  Class: " + className);
        }
        ClassDefinition cdef = (ClassDefinition)DistributedClassLoader.getClasses().get(className);
        if (logger.isDebugEnabled()) {
          logger.debug("    Code Size: " + cdef.getByteCode().length);
        }
      }
    }
  }
  
  public static void testDeploy(WALoader bl, String path, String name)
  {
    try
    {
      bl.addJar("Global", path, name);
    }
    catch (Exception e)
    {
      logger.error("Problem deploying jar", e);
    }
    outputBundles(bl);
  }
  
  public static void testUndeploy(WALoader bl, String name)
  {
    try
    {
      bl.removeJar("Global", name);
    }
    catch (Exception e)
    {
      logger.error("Problem undeploying jar", e);
    }
    outputBundles(bl);
  }
  
  public static void testAddType(WALoader bl)
  {
    Map<String, String> fields = new LinkedHashMap();
    fields.put("merchantId", "java.lang.String");
    fields.put("count", "int");
    fields.put("amount", "double");
    fields.put("other", "com.x.test.UsefulObject");
    try
    {
      bl.addTypeClass("MyApp", "myapp.MyClass", fields);
    }
    catch (Exception e)
    {
      logger.error("Problem adding type", e);
    }
    outputBundles(bl);
  }
  
  public static void testAddType2(WALoader bl)
  {
    Map<String, String> fields = new LinkedHashMap();
    fields.put("event", "com.bloom.event.Event");
    fields.put("myClass", "myapp.MyClass");
    try
    {
      bl.addTypeClass("MyApp", "myapp.MyOtherClass", fields);
    }
    catch (Exception e)
    {
      logger.error("Problem adding type", e);
    }
    outputBundles(bl);
  }
  
  public static void testRemoveType(WALoader bl)
  {
    try
    {
      bl.removeTypeClass("MyApp", "myapp.MyClass");
    }
    catch (Exception e)
    {
      logger.error("Problem removing type", e);
    }
    outputBundles(bl);
  }
  
  public static final void main(String[] args)
  {
    WALoader bl = WALoader.get(true);
    testDeploy(bl, "/Users/steve/Code/Bloom/LoadTestSample1/loadTestSample1.jar", "loadTestSample");
    testAddType(bl);
    testAddType2(bl);
    testUndeploy(bl, "loadTestSample");
    testRemoveType(bl);
    testAddType(bl);
    testDeploy(bl, "/Users/steve/Code/Bloom/LoadTestSample2/loadTestSample2.jar", "loadTestSample");
    testUndeploy(bl, "loadTestSample");
    testDeploy(bl, "/Users/steve/Code/Bloom/LoadTestSample2/loadTestSample2.jar", "loadTestSample");
    testDeploy(bl, "/Users/steve/Code/Bloom/LoadTestSample2/loadTestSample2.jar", "loadTestSample2");
    System.exit(0);
  }
}
