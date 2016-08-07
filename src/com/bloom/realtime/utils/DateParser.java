package com.bloom.runtime.utils;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class DateParser
{
  PartDesc[] parts;
  
  static enum PartType
  {
    unknown,  punc,  year,  month,  day,  hour,  minute,  second,  milli,  zone1,  zone2;
    
    private PartType() {}
  }
  
  class PartDesc
  {
    DateParser.PartType type;
    int length = 0;
    String chars = "";
    
    PartDesc() {}
    
    public int parseInt(char[] chars, int offset, int len)
    {
      int ret = 0;
      for (int i = offset; i < offset + len; i++)
      {
        ret *= 10;
        char c = chars[i];
        int val = c - '0';
        if ((val < 0) || (val > 9)) {
          throw new RuntimeException("Illegal value parsing " + this.type + " '" + c + "' in " + new String(chars, offset, len));
        }
        ret += val;
      }
      return ret;
    }
    
    public int parse(char[] chars, int offset)
    {
      int ret = 0;
      switch (this.type.ordinal())
      {
      case 1: 
        if ((this.length == 2) || (this.length == 1)) {
          return parseInt(chars, offset, this.length);
        }
        break;
      case 2: 
        if ((this.length == 2) || (this.length == 1)) {
          return parseInt(chars, offset, this.length);
        }
        break;
      case 3: 
        if ((this.length > 0) && (this.length < 4)) {
          return parseInt(chars, offset, this.length);
        }
        break;
      case 4: 
        if ((this.length == 2) || (this.length == 1)) {
          return parseInt(chars, offset, this.length);
        }
        break;
      case 5: 
        if ((this.length == 2) || (this.length == 1)) {
          return parseInt(chars, offset, this.length);
        }
        if (this.length == 3) {
          if ((chars[offset] == 'A') || (chars[offset] == 'a'))
          {
            if ((chars[(offset + 1)] == 'P') || (chars[(offset + 1)] == 'p')) {
              return 4;
            }
            if ((chars[(offset + 1)] == 'U') || (chars[(offset + 1)] == 'u')) {
              return 8;
            }
          }
          else
          {
            if ((chars[offset] == 'D') || (chars[offset] == 'd')) {
              return 12;
            }
            if ((chars[offset] == 'F') || (chars[offset] == 'f')) {
              return 2;
            }
            if ((chars[offset] == 'J') || (chars[offset] == 'j'))
            {
              if ((chars[(offset + 1)] == 'A') || (chars[(offset + 1)] == 'a')) {
                return 1;
              }
              if ((chars[(offset + 1)] == 'U') || (chars[(offset + 1)] == 'u'))
              {
                if ((chars[(offset + 2)] == 'N') || (chars[(offset + 2)] == 'n')) {
                  return 6;
                }
                if ((chars[(offset + 2)] == 'L') || (chars[(offset + 2)] == 'l')) {
                  return 7;
                }
              }
            }
            else if ((chars[offset] == 'M') || (chars[offset] == 'm'))
            {
              if ((chars[(offset + 2)] == 'R') || (chars[(offset + 2)] == 'r')) {
                return 3;
              }
              if ((chars[(offset + 2)] == 'Y') || (chars[(offset + 2)] == 'y')) {
                return 5;
              }
            }
            else
            {
              if ((chars[offset] == 'N') || (chars[offset] == 'n')) {
                return 11;
              }
              if ((chars[offset] == 'O') || (chars[offset] == 'o')) {
                return 10;
              }
              if ((chars[offset] == 'S') || (chars[offset] == 's')) {
                return 9;
              }
            }
          }
        }
        throw new IllegalArgumentException("Invalid month value in " + new String(chars));
      case 6: 
        if ((this.length == 2) || (this.length == 1)) {
          return parseInt(chars, offset, this.length);
        }
        break;
      case 7: 
        if ((this.length > 0) && (this.length < 5))
        {
          int y = parseInt(chars, offset, this.length);
          if (this.length <= 2) {
            y = y < 33 ? 2000 + y : 1900 + y;
          }
          return y;
        }
        break;
      case 8: 
        break;
      case 9: 
        break;
      case 10: 
        break;
      case 11: 
        break;
      }
      return ret;
    }
  }
  
  public DateParser(String format)
  {
    char[] formatChars = format.toCharArray();
    
    List<PartDesc> partList = new ArrayList();
    for (char c : formatChars)
    {
      PartDesc currPart = null;
      if (partList.size() > 0) {
        currPart = (PartDesc)partList.get(partList.size() - 1);
      }
      PartType type = PartType.unknown;
      switch (c)
      {
      case 'y': 
        type = PartType.year; break;
      case 'M': 
        type = PartType.month; break;
      case 'd': 
        type = PartType.day; break;
      case 'H': 
        type = PartType.hour; break;
      case 'm': 
        type = PartType.minute; break;
      case 's': 
        type = PartType.second; break;
      case 'S': 
        type = PartType.milli; break;
      case 'z': 
        type = PartType.zone1; break;
      case 'Z': 
        type = PartType.zone2; break;
      case ' ': 
      case '-': 
      case '.': 
      case '/': 
      case ':': 
      case 'T': 
      case '_': 
        type = PartType.punc; break;
      default: 
        throw new IllegalArgumentException("Date format character '" + c + "' in " + format + " is not permitted");
      }
      if ((currPart == null) || (!type.equals(currPart.type)))
      {
        currPart = new PartDesc();
        currPart.type = type;
        partList.add(currPart);
      }
      currPart.length += 1;
      currPart.chars += c;
    }
    this.parts = new PartDesc[partList.size()];
    this.parts = ((PartDesc[])partList.toArray(this.parts));
  }
  
  private String lastDate = null;
  private DateTime lastDateTime = null;
  
  public DateTime parse(String date)
  {
    synchronized (this.parts)
    {
      if (date.equals(this.lastDate)) {
        return this.lastDateTime;
      }
    }
    int y = 0;int M = 0;int d = 0;int h = 0;int m = 0;int s = 0;int S = 0;
    
    char[] chars = date.toCharArray();
    int offset = 0;
    int partIndx = 0;
    PartDesc currPart = this.parts[partIndx];
    try
    {
      while (offset < chars.length)
      {
        switch (currPart.type)
        {
        case day: 
          d = currPart.parse(chars, offset);
          break;
        case hour: 
          h = currPart.parse(chars, offset);
          break;
        case milli: 
          S = currPart.parse(chars, offset);
          break;
        case minute: 
          m = currPart.parse(chars, offset);
          break;
        case month: 
          M = currPart.parse(chars, offset);
          break;
        case punc: 
          break;
        case second: 
          s = currPart.parse(chars, offset);
          break;
        case year: 
          y = currPart.parse(chars, offset);
          break;
        }
        offset += currPart.length;
        partIndx++;
        if (partIndx < this.parts.length) {
          currPart = this.parts[partIndx];
        }
      }
    }
    catch (Throwable t)
    {
      throw new RuntimeException("Problem parsing date " + date + " using format " + toString(), t);
    }
    DateTime ret = new DateTime(y, M, d, h, m, s, S);
    synchronized (this.parts)
    {
      this.lastDate = date;
      this.lastDateTime = ret;
    }
    return ret;
  }
  
  public String toString()
  {
    String ret = "";
    for (PartDesc part : this.parts) {
      ret = ret + part.chars;
    }
    return ret;
  }
  
  public static void main(String[] args)
  {
    DateParser dps = new DateParser("yyyy-MMM-ddTHH:mm:ss.SSS Z");
    
    String[] months = { "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec" };
    for (String m : months)
    {
      String date = "2015-" + m + "-27T18:01:35.000 Z";
      DateTime ds = dps.parse(date);
      System.out.println("Date parser " + dps + " for " + date + " returns " + ds);
      date = "2015-" + m.toUpperCase() + "-27T18:01:35.000 Z";
      ds = dps.parse(date);
      System.out.println("Date parser " + dps + " for " + date + " returns " + ds);
    }
    DateParser dp = new DateParser("yyyy/MM/dd HH:mm:ss.SSS");
    DateTime d = dp.parse("2013/11/05 16:16:16.160");
    System.out.println("Date parser " + dp + " returns " + d);
    DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss.SSS");
    d = dtf.parseDateTime("2013/11/05 16:16:16.160");
    System.out.println("DateTimeFormat returns " + d);
    
    long numRuns = 10000000L;
    System.out.println("Warming DateTimeFormat");
    for (int i = 0; i < numRuns; i++) {
      d = dtf.parseDateTime("2013/11/05 16:16:16.160");
    }
    System.out.println("Warming DateParser");
    for (int i = 0; i < numRuns; i++) {
      d = dp.parse("2013/11/05 16:16:16.160");
    }
    long stime = System.currentTimeMillis();
    System.out.println("Running DateTimeFormat");
    for (int i = 0; i < numRuns; i++) {
      d = dtf.parseDateTime("2013/11/05 16:16:16.160");
    }
    long etime = System.currentTimeMillis();
    long diff = etime - stime;
    long rate = numRuns * 1000L / diff;
    System.out.println("Completed DateTimeFormat in " + diff + " ms rate " + rate + " per/s");
    
    stime = System.currentTimeMillis();
    System.out.println("Running DateParser");
    for (int i = 0; i < numRuns; i++) {
      d = dp.parse("2013/11/05 16:16:16.160");
    }
    etime = System.currentTimeMillis();
    diff = etime - stime;
    rate = numRuns * 1000L / diff;
    System.out.println("Completed DateParser in " + diff + " ms rate " + rate + " per/s");
  }
}
