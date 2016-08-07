package com.bloom.runtime.utils;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;

public class NamePolicy
{
  private static NamePolicyImpl policy = new IgnoreCaseNamePolicyImpl();
  
  public static Field getField(Class<?> type, String fieldName)
    throws NoSuchFieldException, SecurityException
  {
    return policy.getField(type, fieldName);
  }
  
  public static <V> HashMap<String, V> makeNameMap()
  {
    return new CaseInsensitiveHashMap();
  }
  
  public static <V> LinkedHashMap<String, V> makeNameLinkedMap()
  {
    return new CaseInsensitiveLinkedHashMap();
  }
  
  public static HashSet<String> makeNameSet()
  {
    return new CaseInsensitiveSet();
  }
  
  public static boolean isEqual(String name1, String name2)
  {
    return policy.isEqual(name1, name2);
  }
  
  public static String makeKey(String s)
  {
    return s.toUpperCase();
  }
  
  static abstract interface NamePolicyImpl
  {
    public abstract Field getField(Class<?> paramClass, String paramString)
      throws NoSuchFieldException, SecurityException;
    
    public abstract boolean isEqual(String paramString1, String paramString2);
  }
  
  static class DefaultNamePolicyImpl
    implements NamePolicy.NamePolicyImpl
  {
    public Field getField(Class<?> type, String fieldName)
      throws NoSuchFieldException, SecurityException
    {
      Field ret = type.getField(fieldName);
      return ret;
    }
    
    public boolean isEqual(String name1, String name2)
    {
      return name1.equals(name2);
    }
  }
  
  static class IgnoreCaseNamePolicyImpl
    implements NamePolicy.NamePolicyImpl
  {
    public Field getField(Class<?> type, String fieldName)
      throws NoSuchFieldException, SecurityException
    {
      for (Field f : type.getFields()) {
        if (f.getName().equalsIgnoreCase(fieldName)) {
          return f;
        }
      }
      throw new NoSuchFieldException(fieldName);
    }
    
    public boolean isEqual(String name1, String name2)
    {
      return name1.equalsIgnoreCase(name2);
    }
  }
  
  private static class CaseInsensitiveHashMap<V>
    extends HashMap<String, V>
  {
    private static final long serialVersionUID = -1456385725381468246L;
    
    public V get(Object key)
    {
      if ((key instanceof String)) {
        return (V)super.get(NamePolicy.makeKey((String)key));
      }
      return (V)super.get(key);
    }
    
    public boolean containsKey(Object key)
    {
      if ((key instanceof String)) {
        return super.containsKey(NamePolicy.makeKey((String)key));
      }
      return super.containsKey(key);
    }
    
    public V remove(Object key)
    {
      if ((key instanceof String)) {
        return (V)super.remove(NamePolicy.makeKey((String)key));
      }
      return (V)super.remove(key);
    }
    
    public V put(String key, V value)
    {
      return (V)super.put(NamePolicy.makeKey(key), value);
    }
  }
  
  private static class CaseInsensitiveLinkedHashMap<V>
    extends LinkedHashMap<String, V>
  {
    private static final long serialVersionUID = -1456385725381468246L;
    
    public V get(Object key)
    {
      if ((key instanceof String)) {
        return (V)super.get(NamePolicy.makeKey((String)key));
      }
      return (V)super.get(key);
    }
    
    public boolean containsKey(Object key)
    {
      if ((key instanceof String)) {
        return super.containsKey(NamePolicy.makeKey((String)key));
      }
      return super.containsKey(key);
    }
    
    public V remove(Object key)
    {
      if ((key instanceof String)) {
        return (V)super.remove(NamePolicy.makeKey((String)key));
      }
      return (V)super.remove(key);
    }
    
    public V put(String key, V value)
    {
      return (V)super.put(NamePolicy.makeKey(key), value);
    }
  }
  
  private static class CaseInsensitiveSet
    extends HashSet<String>
  {
    private static final long serialVersionUID = -1456385725381468246L;
    
    public boolean contains(Object o)
    {
      if ((o instanceof String)) {
        return super.contains(NamePolicy.makeKey((String)o));
      }
      return super.contains(o);
    }
    
    public boolean remove(Object o)
    {
      if ((o instanceof String)) {
        return super.remove(NamePolicy.makeKey((String)o));
      }
      return super.remove(o);
    }
    
    public boolean add(String key)
    {
      return super.add(NamePolicy.makeKey(key));
    }
  }
}
