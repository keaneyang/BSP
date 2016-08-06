package com.bloom.preview;

import java.util.HashMap;
import java.util.Map;

public enum ParserType
{
  DSV("DSVPARSER"),  XML("XMLPARSER"),  JSON("JSONPARSER"),  FreeFormText("FreeFormTextParser");
  
  String parserName = null;
  
  private ParserType(String parserName)
  {
    this.parserName = parserName;
  }
  
  public String getParserName()
  {
    return this.parserName;
  }
  
  public abstract String getForClassName();
  
  public abstract Map<String, Object> getDefaultParserProperties();
}
