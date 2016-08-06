package com.bloom.classloading;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.NotFoundException;
import org.apache.log4j.Logger;

import com.bloom.ser.KryoSingleton;

public class BundleLoader
  extends DistributedClassLoader
{
  static Logger logger = Logger.getLogger(BundleLoader.class);
  Map<String, Class<?>> loadedClasses;
  
  static class NoLongerValid {}
  
  class BundlePool
    extends ClassPool
  {
    BundleLoader peerLoader;
    Map<String, CtClass> loadedCtClasses;
    
    public BundlePool(BundleLoader bLoader, ClassPool parent)
    {
      super();
      this.peerLoader = bLoader;
      this.loadedCtClasses = new HashMap();
    }
    
    public CtClass get(String name)
      throws NotFoundException
    {
      if (BundleLoader.logger.isTraceEnabled()) {
        BundleLoader.logger.trace(this.peerLoader.getName() + " get CtClass " + name);
      }
      synchronized (this.loadedCtClasses)
      {
        try
        {
          CtClass sClazz = super.get(name);
          if (sClazz != null)
          {
            if (BundleLoader.logger.isTraceEnabled()) {
              BundleLoader.logger.trace(this.peerLoader.getName() + " found CtClass " + name + " in cache");
            }
            return sClazz;
          }
        }
        catch (NotFoundException nfe)
        {
          if (BundleLoader.logger.isTraceEnabled()) {
            BundleLoader.logger.trace(this.peerLoader.getName() + " CtClass " + name + " not in cache");
          }
        }
        if (this.loadedCtClasses.containsKey(name))
        {
          if (BundleLoader.logger.isTraceEnabled()) {
            BundleLoader.logger.trace(this.peerLoader.getName() + " CtClass " + name + " in previously loaded classes");
          }
          return (CtClass)this.loadedCtClasses.get(name);
        }
        ClassDefinition def = this.peerLoader.getClassDefinition(name);
        if (def != null)
        {
          ByteArrayInputStream bais = new ByteArrayInputStream(def.getByteCode());
          CtClass ctClazz;
          try
          {
            ctClazz = makeClass(bais);
          }
          catch (IOException|RuntimeException e)
          {
            throw new RuntimeException(this.peerLoader.getName() + ": Invalid bytecode for class " + name, e);
          }
          this.loadedCtClasses.put(name, ctClazz);
          if (BundleLoader.logger.isTraceEnabled()) {
            BundleLoader.logger.trace(this.peerLoader.getName() + " CtClass " + name + " created from byte code");
          }
          return ctClazz;
        }
      }
      if (BundleLoader.logger.isTraceEnabled()) {
        BundleLoader.logger.trace(this.peerLoader.getName() + " asking parent for CtClass " + name);
      }
      return this.parent.get(name);
    }
  }
  
  long ts = System.currentTimeMillis();
  
  public BundleLoader(ClassLoader parent, boolean isClient)
  {
    super(parent, isClient);
    this.pool = new BundlePool(this, ((DistributedClassLoader)getParent()).pool);
    this.pool.childFirstLookup = true;
    this.loadedClasses = new HashMap();
  }
  
  protected Class<?> loadClass(String name, boolean resolve)
    throws ClassNotFoundException
  {
    if (logger.isTraceEnabled()) {
      logger.trace(getName() + ": load class " + name);
    }
    synchronized (this.loadedClasses)
    {
      if (this.loadedClasses.containsKey(name))
      {
        Class<?> clazz = (Class)this.loadedClasses.get(name);
        if (NoLongerValid.class.equals(clazz)) {
          return getParent().loadClass(name);
        }
        return clazz;
      }
    }
    ClassDefinition def = getClassDefinition(name);
    if ((def != null) && (def.getBundleName().equals(getName())))
    {
      Class<?> clazz = defineClass(name, getName(), def.getByteCode(), 0, def.getByteCode().length);
      if (def.getClassId() != -1) {
        KryoSingleton.get().addClassRegistration(clazz, def.getClassId());
      }
      return clazz;
    }
    return getParent().loadClass(name);
  }
  
  public Class<?> defineClass(String name, String loaderName, byte[] b, int off, int len)
    throws ClassFormatError
  {
    Class<?> clazz = null;
    synchronized (this.loadedClasses)
    {
      if (this.loadedClasses.containsKey(name))
      {
        if (NoLongerValid.class.equals(clazz)) {
          clazz = ((WALoader)getParent()).defineClass(name, loaderName, b, off, len);
        } else {
          clazz = (Class)this.loadedClasses.get(name);
        }
      }
      else
      {
        clazz = defineClass(name, b, off, len);
        this.loadedClasses.put(name, clazz);
      }
    }
    return clazz;
  }
  
  public String toString()
  {
    return getName() + "-" + this.ts;
  }
  
  void removeClasses()
  {
    synchronized (this.loadedClasses)
    {
      for (Map.Entry<String, Class<?>> entry : this.loadedClasses.entrySet()) {
        if (!((Class)entry.getValue()).equals(NoLongerValid.class))
        {
          KryoSingleton.get().removeClassRegistration((Class)entry.getValue());
          this.loadedClasses.put(entry.getKey(), NoLongerValid.class);
        }
      }
    }
  }
}
