package com.bloom.preview;

import java.util.Map;

public abstract interface ReaderProperties
{
  public abstract Map<String, Object> getDefaultProperties();
  
  public abstract Map<String, String> getDefaultPropertiesAsString();
}

