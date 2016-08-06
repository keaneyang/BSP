package com.bloom.intf;

import java.util.Map;

import com.bloom.runtime.compiler.TypeDefOrName;

public abstract interface SourceMetadataProvider
{
  public abstract String getMetadataKey();
  
  public abstract Map<String, TypeDefOrName> getMetadata()
    throws Exception;
}

