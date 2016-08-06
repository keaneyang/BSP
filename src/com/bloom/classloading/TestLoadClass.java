package com.bloom.classloading;

import java.io.PrintStream;
import java.lang.reflect.Method;
import javassist.ClassPool;
import javassist.CtClass;
import org.apache.log4j.Logger;

public class TestLoadClass
{
  private static Logger logger = Logger.getLogger(TestDeploy.class);
  
  public static void testLoadCreateClass(WALoader bl)
  {
    try
    {
      System.out.println("Get javassist class com.x.test.ObjectFactory");
      CtClass poolClass = bl.pool.get("com.x.test.ObjectFactory");
      System.out.println("Got javassist class com.x.test.ObjectFactory " + poolClass);
      
      System.out.println("Get class com.x.test.ObjectFactory");
      Class<?> clazz = bl.loadClass("com.x.test.ObjectFactory");
      if (clazz == null) {
        return;
      }
      System.out.println("Create new instance of com.x.test.ObjectFactory");
      Object objectFactory = clazz.newInstance();
      System.out.println("Get getUsefulObject method");
      Method getUsefulObject = clazz.getMethod("getUsefulObject", new Class[] { (Class)null });
      if (getUsefulObject == null) {
        return;
      }
      System.out.println("Invoke getUsefulObject method");
      Object usefulObject = getUsefulObject.invoke(objectFactory, new Object[] { (Class)null });
      System.out.println("Got UsefulObject " + usefulObject);
      if (usefulObject == null) {
        return;
      }
      Method getVersion = usefulObject.getClass().getMethod("getVersion", new Class[] { (Class)null });
      if (getVersion == null) {
        return;
      }
      System.out.println("Invoke getVersion method");
      Object version = getVersion.invoke(usefulObject, new Object[] { (Class)null });
      System.out.println("Got version " + version);
    }
    catch (Exception e)
    {
      logger.error("Problem loading and creating object", e);
    }
  }
  
  public static final void main(String[] args)
  {
    WALoader bl = WALoader.get(true);
    testLoadCreateClass(bl);
    testLoadCreateClass(bl);
    testLoadCreateClass(bl);
    testLoadCreateClass(bl);
    System.exit(0);
  }
}
