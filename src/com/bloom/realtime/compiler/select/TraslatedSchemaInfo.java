package com.bloom.runtime.compiler.select;

import com.bloom.uuid.UUID;

public abstract class TraslatedSchemaInfo
{
  public abstract UUID expectedTypeID();
  
  public abstract String toString();
}

