package com.bloom.runtime.compiler;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.bloom.runtime.utils.Factory;

public class Imports
{
  private final Map<String, Field> staticVars = Factory.makeNameMap();
  private final Map<String, Class<?>> classes = Factory.makeNameMap();
  private final Map<String, List<Class<?>>> staticMethods = Factory.makeNameMap();
  
  public Class<?> findClass(String name)
  {
    return (Class)this.classes.get(name);
  }
  
  public void importPackage(String pkgname)
  {
    if (!$assertionsDisabled) {
      throw new AssertionError("not implemented yet");
    }
  }
  
  public Class<?> importClass(Class<?> c)
  {
    Class<?> pc = (Class)this.classes.get(c.getSimpleName());
    if ((pc == null) || (pc.equals(c)))
    {
      this.classes.put(c.getSimpleName(), c);
      return null;
    }
    return c;
  }
  
  public Field getStaticFieldRef(String name)
  {
    return (Field)this.staticVars.get(name);
  }
  
  public Field addStaticFieldRef(Field f)
  {
    Field pf = (Field)this.staticVars.get(f.getName());
    if ((pf == null) || (pf.equals(f)))
    {
      this.staticVars.put(f.getName(), f);
      return null;
    }
    return pf;
  }
  
  public List<Class<?>> getStaticMethodRef(String name, List<Class<?>> firstLookHere)
  {
    List<Class<?>> list = (List)this.staticMethods.get(name);
    if (list != null)
    {
      List<Class<?>> ret = new ArrayList(firstLookHere);
      ret.addAll(list);
      return ret;
    }
    return firstLookHere;
  }
  
  public List<Class<?>> getStaticMethodRef(String name, Class<?>... firstLookHere)
  {
    return getStaticMethodRef(name, Arrays.asList(firstLookHere));
  }
  
  public void addStaticMethod(Class<?> c, String name)
  {
    List<Class<?>> l = (List)this.staticMethods.get(name);
    if (l == null)
    {
      l = new LinkedList();
      l.add(c);
      this.staticMethods.put(name, l);
    }
    else if (!l.contains(c))
    {
      l.add(c);
    }
  }
}
