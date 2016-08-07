package com.bloom.runtime.compiler.patternmatch;

public enum TimerFunc
{
  CREATE("timer"),  STOP("stoptimer"),  WAIT("wait");
  
  private final String funcName;
  
  private TimerFunc(String funcName)
  {
    this.funcName = funcName;
  }
  
  public String getFuncName()
  {
    return this.funcName;
  }
}
