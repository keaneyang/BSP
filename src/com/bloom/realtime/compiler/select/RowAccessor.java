package com.bloom.runtime.compiler.select;

public abstract class RowAccessor
{
  public abstract String genRowAccessor(String paramString);
  
  public abstract String toString();
}

