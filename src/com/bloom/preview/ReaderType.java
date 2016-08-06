package com.bloom.preview;

import java.util.HashMap;
import java.util.Map;

public enum ReaderType
{
  FileReader,  HDFSReader;
  
  private ReaderType() {}
  
  public abstract Map<String, Object> getDefaultReaderProperties();
  
  public abstract Map<String, String> getDefaultReaderPropertiesAsString();
}

