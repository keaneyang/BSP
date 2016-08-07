package com.bloom.runtime.compiler.select;

import com.bloom.runtime.compiler.exprs.FieldRef;

public abstract interface IteratorInfo
{
  public abstract DataSet getParent();
  
  public abstract FieldRef getIterableField();
}

