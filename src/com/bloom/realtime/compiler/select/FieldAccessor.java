package com.bloom.runtime.compiler.select;

import com.bloom.runtime.compiler.exprs.VirtFieldRef;

public abstract class FieldAccessor
{
  public abstract String genFieldAccessor(String paramString, VirtFieldRef paramVirtFieldRef);
  
  public abstract String toString();
}
