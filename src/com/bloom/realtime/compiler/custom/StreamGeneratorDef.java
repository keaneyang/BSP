package com.bloom.runtime.compiler.custom;

import com.bloom.uuid.UUID;

public class StreamGeneratorDef
{
  public final UUID outputTypeID;
  public final Class<?> outputType;
  public final Object[] args;
  public final String generatorClassName;
  
  public StreamGeneratorDef(UUID outputTypeID, Class<?> outputType, Object[] args, String generatorClassName)
  {
    this.outputTypeID = outputTypeID;
    this.outputType = outputType;
    this.args = ((Object[])args.clone());
    this.generatorClassName = generatorClassName;
  }
}
