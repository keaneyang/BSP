package com.bloom.runtime.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.TreeMap;

public class Factory
{
  public static <K, V> HashMap<K, V> makeMap()
  {
    return new HashMap();
  }
  
  public static TreeMap<String, Object> makeCaseInsensitiveMap()
  {
    return new TreeMap(String.CASE_INSENSITIVE_ORDER);
  }
  
  public static <K, V> LinkedHashMap<K, V> makeLinkedMap()
  {
    return new LinkedHashMap();
  }
  
  public static <V> HashMap<String, V> makeNameMap()
  {
    return NamePolicy.makeNameMap();
  }
  
  public static <V> LinkedHashMap<String, V> makeNameLinkedMap()
  {
    return NamePolicy.makeNameLinkedMap();
  }
  
  public static HashSet<String> makeNameSet()
  {
    return NamePolicy.makeNameSet();
  }
  
  public static <T> List<T> makeList()
  {
    return new ArrayList();
  }
}
