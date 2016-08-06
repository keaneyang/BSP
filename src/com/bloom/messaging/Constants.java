package com.bloom.messaging;

public class Constants
{
  public static final String TCP = "tcp://";
  public static final String IPC = "ipc://";
  public static final String INPROC = "inproc://";
  public static final String SEPARATOR = ":";
  public static final String HYPHENSEPARATOR = "-";
  public static final long NANOSECONDS_PER_SECOND = 1000000000L;
  public static final int MB = 1048576;
  public static final String NOVAL = "NOVAL";
  
  public static boolean toBoolean(byte[] data)
  {
    return (data != null) && (data.length != 0);
  }
  
  public static float ns2s(long nsecs)
  {
    float f = (float)nsecs;
    return f / 1.0E9F;
  }
  
  public static float b2mb(long bytes)
  {
    float bb = (float)bytes;
    float ll = 1048576.0F;
    return bb / ll;
  }
  
  public static String stringIndexer(String ss, String delimiter, int start, int end)
  {
    if (ss == null) {
      throw new IllegalArgumentException("Can't Index into a NULL String");
    }
    if (delimiter == null) {
      throw new NullPointerException("Can't take NULL as delimiter");
    }
    if ((start < 0) || (end < 0) || (start > ss.length()) || (end > ss.length())) {
      throw new ArrayIndexOutOfBoundsException("Can't Index to the String, Please check start and end values");
    }
    int firstIndex = ss.indexOf(":");
    int lastIndex = ss.lastIndexOf(":");
    return ss.substring(firstIndex + 1, lastIndex);
  }
}
