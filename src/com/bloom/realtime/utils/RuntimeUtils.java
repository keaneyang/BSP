package com.bloom.runtime.utils;

import com.bloom.uuid.UUIDGen;

public class RuntimeUtils
{
  public static String genRandomName(String prefix)
  {
    long a = UUIDGen.newTime();
    long b = UUIDGen.getClockSeqAndNode();
    return (prefix == null ? "X" : prefix) + Long.toHexString(a) + Long.toHexString(b);
  }
}
