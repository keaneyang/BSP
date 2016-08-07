package com.bloom.runtime.utils;

public class FieldToObject
{
  public static Object convert(Object obj)
  {
    return obj;
  }
  
  public static Object convert(boolean obj)
  {
    return Boolean.valueOf(obj);
  }
  
  public static Object convert(byte obj)
  {
    return Byte.valueOf(obj);
  }
  
  public static Object convert(char obj)
  {
    return Character.valueOf(obj);
  }
  
  public static Object convert(short obj)
  {
    return Short.valueOf(obj);
  }
  
  public static Object convert(long obj)
  {
    return Long.valueOf(obj);
  }
  
  public static Object convert(int obj)
  {
    return Integer.valueOf(obj);
  }
  
  public static Object convert(float obj)
  {
    return Float.valueOf(obj);
  }
  
  public static Object convert(double obj)
  {
    return Double.valueOf(obj);
  }
  
  public static String genConvert(String arg)
  {
    return FieldToObject.class.getName() + ".convert(" + arg + ")";
  }
}
