package com.bloom.runtime.compiler.stmts;

import java.io.Serializable;

public class RecoveryDescription
  implements Serializable
{
  public static final String PARAM = "RecoveryDescription";
  public static final String SIGNALS_NAME = "StartupSignalsSetName";
  public static final String SIGNALS_PREFIX = "StartupSignals";
  public final int type;
  public final long interval;
  
  private RecoveryDescription()
  {
    this.type = 0;
    this.interval = 0L;
  }
  
  public RecoveryDescription(int type, long interval)
  {
    this.type = type;
    this.interval = interval;
  }
}
