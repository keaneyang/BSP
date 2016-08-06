package com.bloom.waction;

import org.apache.log4j.Logger;

public class WactionContextDefault
  extends WactionContext
{
  private static final long serialVersionUID = 2930824976792265751L;
  private static transient Logger logger = Logger.getLogger(WactionContextDefault.class);
  
  public Object get(Object key)
  {
    return null;
  }
  
  public Object put(String key, Object value)
  {
    return null;
  }
}
