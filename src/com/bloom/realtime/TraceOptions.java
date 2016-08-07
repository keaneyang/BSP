package com.bloom.runtime;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

public class TraceOptions
  implements Serializable
{
  private static final long serialVersionUID = 6782882324928737147L;
  public final int traceFlags;
  public final String traceFilename;
  public final String traceFilePath;
  
  public TraceOptions()
  {
    this.traceFlags = 0;
    this.traceFilename = null;
    this.traceFilePath = null;
  }
  
  public TraceOptions(int traceFlags, String traceFilename, String traceFilePath)
  {
    this.traceFlags = traceFlags;
    this.traceFilename = traceFilename;
    this.traceFilePath = traceFilePath;
  }
  
  public String toString()
  {
    return "TraceOptions(" + this.traceFlags + "," + this.traceFilename + ")";
  }
  
  private static final Map<String, PrintStream> cachedOpenTraceStreams = new LinkedHashMap(4, 0.75F, true)
  {
    private static final long serialVersionUID = 1L;
    
    protected boolean removeEldestEntry(Map.Entry eldest)
    {
      return size() > 4;
    }
  };
  
  public static PrintStream getTraceStream(TraceOptions opt)
  {
    String name = opt.traceFilename;
    if (name == null) {
      return System.out;
    }
    PrintStream res = (PrintStream)cachedOpenTraceStreams.get(name);
    if (res == null)
    {
      try
      {
        res = new PrintStream(new FileOutputStream(name), true);
      }
      catch (FileNotFoundException e)
      {
        e.printStackTrace();
        return System.out;
      }
      cachedOpenTraceStreams.put(name, res);
    }
    return res;
  }
}
