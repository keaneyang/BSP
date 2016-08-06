package com.bloom.intf;

import java.util.List;
import java.util.Map;

public abstract interface Analyzer
{
  public abstract Map<String, Object> getFileDetails();
  
  public abstract List<Map<String, Object>> getProbableProperties();
}

