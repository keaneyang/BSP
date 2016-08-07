package com.bloom.runtime.compiler.select;

import com.bloom.runtime.meta.CQExecutionPlan;

public abstract class Generator
{
  public abstract CQExecutionPlan generate()
    throws Exception;
}
