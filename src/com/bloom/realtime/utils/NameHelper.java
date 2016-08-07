package com.bloom.runtime.utils;

import java.util.List;

import com.bloom.runtime.utils.StringUtils;

public class NameHelper
{
  public static String toString(List<String> list)
  {
    return StringUtils.join(list, ".");
  }
  
  public static String getPrefix(String name)
  {
    int dot = name.lastIndexOf('.');
    if (dot == -1) {
      return "";
    }
    return name.substring(0, dot);
  }
  
  public static String getBasename(String name)
  {
    int dot = name.lastIndexOf('.');
    if (dot == -1) {
      return name;
    }
    return name.substring(dot + 1);
  }
}
