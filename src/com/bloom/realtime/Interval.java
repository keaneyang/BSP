package com.bloom.runtime;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Interval
  implements Serializable
{
  private static final long serialVersionUID = -1040195849007951879L;
  private static Pattern numPat = Pattern.compile("(\\d{1,9})");
  private static Pattern yearmonthPat = Pattern.compile("(\\d{1,9})-(\\d{1,2})");
  private static Pattern daysecPat = Pattern.compile("(\\d{1,9})\\s+(\\d{1,2}):(\\d{1,2}):(\\d{1,2})(\\.(\\d{1,6}))?");
  private static Pattern dayhourPat = Pattern.compile("(\\d{1,9})\\s+(\\d{1,2})");
  private static Pattern dayminPat = Pattern.compile("(\\d{1,9})\\s+(\\d{1,2}):(\\d{1,2})");
  private static Pattern hourminPat = Pattern.compile("(\\d{1,9}):(\\d{1,2})");
  private static Pattern hoursecPat = Pattern.compile("(\\d{1,9}):(\\d{1,2}):(\\d{1,2})(\\.(\\d{1,6}))?");
  private static Pattern minsecPat = Pattern.compile("(\\d{1,9}):(\\d{1,2})(\\.(\\d{1,6}))?");
  private static Pattern secPat = Pattern.compile("(\\d{1,9})(\\.(\\d{1,6}))?");
  private static final int secInMin = 60;
  private static final int secInHour = 3600;
  private static final int secInDay = 86400;
  public final long value;
  
  private static Pattern getYMpat(int flags)
  {
    switch (flags)
    {
    case 16: 
    case 32: 
      return numPat;
    case 48: 
      return yearmonthPat;
    }
    if (!$assertionsDisabled) {
      throw new AssertionError();
    }
    return null;
  }
  
  private static Pattern getDSpat(int flags)
  {
    switch (flags)
    {
    case 1: 
      return secPat;
    case 2: 
      return numPat;
    case 3: 
      return minsecPat;
    case 4: 
      return numPat;
    case 6: 
      return hourminPat;
    case 7: 
      return hoursecPat;
    case 8: 
      return numPat;
    case 12: 
      return dayhourPat;
    case 14: 
      return dayminPat;
    case 15: 
      return daysecPat;
    }
    if (!$assertionsDisabled) {
      throw new AssertionError();
    }
    return null;
  }
  
  public static int parseMicrosecs(String s)
  {
    if (s != null)
    {
      int val = Integer.parseInt(s);
      for (int len = s.length(); len < 6; len++) {
        val *= 10;
      }
      return val;
    }
    return 0;
  }
  
  public Interval()
  {
    this.value = 0L;
  }
  
  public Interval(long val)
  {
    this.value = val;
  }
  
  public String toString()
  {
    long val = this.value;
    long microsec = val % 1000000L;
    val /= 1000000L;
    return String.format("%d.%06d", new Object[] { Long.valueOf(val), Long.valueOf(microsec) });
  }
  
  public static Interval parseDSInterval(String literal, int flags)
  {
    long days = 0L;
    long hours = 0L;
    long minutes = 0L;
    long seconds = 0L;
    long microsecs = 0L;
    Matcher m = getDSpat(flags).matcher(literal);
    if (!m.matches()) {
      return null;
    }
    switch (flags)
    {
    case 1: 
      seconds = Integer.parseInt(m.group(1));
      microsecs = parseMicrosecs(m.group(3));
      break;
    case 2: 
      minutes = Integer.parseInt(m.group(1));
      break;
    case 3: 
      minutes = Integer.parseInt(m.group(1));
      seconds = Integer.parseInt(m.group(2));
      microsecs = parseMicrosecs(m.group(4));
      if (seconds > 59L) {
        return null;
      }
      break;
    case 4: 
      hours = Integer.parseInt(m.group(1));
      break;
    case 6: 
      hours = Integer.parseInt(m.group(1));
      minutes = Integer.parseInt(m.group(2));
      if (minutes > 59L) {
        return null;
      }
      break;
    case 7: 
      hours = Integer.parseInt(m.group(1));
      minutes = Integer.parseInt(m.group(2));
      seconds = Integer.parseInt(m.group(3));
      microsecs = parseMicrosecs(m.group(5));
      if ((minutes > 59L) || (seconds > 59L)) {
        return null;
      }
      break;
    case 8: 
      days = Integer.parseInt(m.group(1));
      break;
    case 12: 
      days = Integer.parseInt(m.group(1));
      hours = Integer.parseInt(m.group(2));
      if (hours > 23L) {
        return null;
      }
      break;
    case 14: 
      days = Integer.parseInt(m.group(1));
      hours = Integer.parseInt(m.group(2));
      minutes = Integer.parseInt(m.group(3));
      if ((hours > 23L) || (minutes > 59L)) {
        return null;
      }
      break;
    case 15: 
      days = Integer.parseInt(m.group(1));
      hours = Integer.parseInt(m.group(2));
      minutes = Integer.parseInt(m.group(3));
      seconds = Integer.parseInt(m.group(4));
      microsecs = parseMicrosecs(m.group(6));
      if ((hours > 23L) || (minutes > 59L) || (seconds > 59L)) {
        return null;
      }
      break;
    case 5: 
    case 9: 
    case 10: 
    case 11: 
    case 13: 
    default: 
      if (!$assertionsDisabled) {
        throw new AssertionError();
      }
      break;
    }
    long secs = days * 86400L + hours * 3600L + minutes * 60L + seconds;
    return new Interval(secs * 1000000L + microsecs);
  }
  
  public static Interval parseYMInterval(String literal, int flags)
  {
    int months = 0;
    int years = 0;
    Matcher m = getYMpat(flags).matcher(literal);
    if (!m.matches()) {
      return null;
    }
    switch (flags)
    {
    case 16: 
      months = Integer.parseInt(m.group(1));
      if ((months < 1) || (months > 12)) {
        return null;
      }
      break;
    case 32: 
      years = Integer.parseInt(m.group(1));
      break;
    case 48: 
      years = Integer.parseInt(m.group(1));
      months = Integer.parseInt(m.group(2));
      if ((months < 1) || (months > 12)) {
        return null;
      }
      break;
    default: 
      if (!$assertionsDisabled) {
        throw new AssertionError();
      }
      break;
    }
    return new Interval(years * 12 + months);
  }
  
  public String toHumanReadable()
  {
    String sign = this.value < 0L ? "-" : "";
    long seconds = Math.abs(this.value / 1000000L);
    long microseconds = Math.abs(this.value % 1000000L);
    if (microseconds == 0L)
    {
      if (seconds >= 60L)
      {
        if (seconds % 86400L == 0L) {
          return String.format("%s%d DAY", new Object[] { sign, Long.valueOf(seconds / 86400L) });
        }
        if (seconds % 3600L == 0L) {
          return String.format("%s%d HOUR", new Object[] { sign, Long.valueOf(seconds / 3600L) });
        }
        if (seconds % 60L == 0L) {
          return String.format("%s%d MINUTE", new Object[] { sign, Long.valueOf(seconds / 60L) });
        }
      }
      return String.format("%s%d SECOND", new Object[] { sign, Long.valueOf(seconds) });
    }
    return String.format("%s%d.%06d SECOND", new Object[] { sign, Long.valueOf(seconds), Long.valueOf(microseconds) });
  }
}
