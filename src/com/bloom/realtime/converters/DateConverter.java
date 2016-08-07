package com.bloom.runtime.converters;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DateConverter
{
  private static Map<String, SimpleDateFormat> sdfs = new ConcurrentHashMap();
  
  public static final Date stringToDate(String value, String format)
  {
    SimpleDateFormat sdf = (SimpleDateFormat)sdfs.get(format);
    if (sdf == null)
    {
      sdf = new SimpleDateFormat(format);
      sdf.setLenient(true);
      sdfs.put(format, sdf);
    }
    try
    {
      return sdf.parse(value);
    }
    catch (ParseException e) {}
    return null;
  }
  
  public static final String dateToString(Date value, String format)
  {
    SimpleDateFormat sdf = (SimpleDateFormat)sdfs.get(format);
    if (sdf == null)
    {
      sdf = new SimpleDateFormat(format);
      sdf.setLenient(true);
      sdfs.put(format, sdf);
    }
    return sdf.format(value);
  }
  
  public static final int stringToInt(String value)
  {
    return Integer.parseInt(value);
  }
  
  public static final double stringToDouble(String value)
  {
    return Double.parseDouble(value);
  }
}
