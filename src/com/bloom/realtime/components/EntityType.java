package com.bloom.runtime.components;

public enum EntityType
{
  UNKNOWN(0),  APPLICATION(1),  STREAM(2),  WINDOW(3),  TYPE(4),  CQ(5),  SOURCE(6),  TARGET(7),  FLOW(8),  PROPERTYSET(9),  WACTIONSTORE(10),  PROPERTYTEMPLATE(11),  CACHE(12),  WI(13),  ALERTSUBSCRIBER(14),  SERVER(15),  USER(16),  ROLE(17),  INITIALIZER(18),  DG(19),  VISUALIZATION(20),  NAMESPACE(22),  EXCEPTIONHANDLER(23),  STREAM_GENERATOR(24),  SORTER(25),  WASTOREVIEW(26),  AGENT(27),  DASHBOARD(28),  PAGE(29),  QUERYVISUALIZATION(30),  QUERY(32),  POSITION(33),  PROPERTYVARIABLE(34);
  
  private final int val;
  
  private EntityType(int v)
  {
    this.val = v;
  }
  
  public int getValue()
  {
    return this.val;
  }
  
  public boolean canBePartOfApp()
  {
    switch (this)
    {
    case TYPE: 
    case STREAM: 
    case STREAM_GENERATOR: 
    case WINDOW: 
    case CACHE: 
    case CQ: 
    case TARGET: 
    case WACTIONSTORE: 
    case SOURCE: 
    case FLOW: 
    case SORTER: 
    case WASTOREVIEW: 
    case VISUALIZATION: 
    case EXCEPTIONHANDLER: 
    case PROPERTYSET: 
      return true;
    }
    return false;
  }
  
  public boolean isAccessible()
  {
    switch (this)
    {
    case CACHE: 
    case WACTIONSTORE: 
    case SOURCE: 
      return true;
    }
    return false;
  }
  
  public boolean canBePartOfFlow()
  {
    switch (this)
    {
    case TYPE: 
    case STREAM: 
    case STREAM_GENERATOR: 
    case WINDOW: 
    case CACHE: 
    case CQ: 
    case TARGET: 
    case WACTIONSTORE: 
    case SOURCE: 
    case FLOW: 
    case SORTER: 
    case WASTOREVIEW: 
    case PROPERTYSET: 
      return true;
    }
    return false;
  }
  
  public boolean isGlobal()
  {
    switch (this)
    {
    case NAMESPACE: 
    case PROPERTYTEMPLATE: 
    case ALERTSUBSCRIBER: 
    case SERVER: 
    case AGENT: 
    case USER: 
    case INITIALIZER: 
    case DG: 
      return true;
    }
    return false;
  }
  
  public boolean isSystem()
  {
    switch (this)
    {
    case INITIALIZER: 
    case UNKNOWN: 
      return true;
    }
    return false;
  }
  
  public boolean isNotVersionable()
  {
    switch (this)
    {
    case ALERTSUBSCRIBER: 
    case SERVER: 
    case USER: 
    case INITIALIZER: 
    case DG: 
    case ROLE: 
    case QUERY: 
      return true;
    }
    return false;
  }
  
  public boolean isStoreable()
  {
    switch (this)
    {
    case ALERTSUBSCRIBER: 
    case SERVER: 
    case AGENT: 
    case INITIALIZER: 
    case UNKNOWN: 
    case WI: 
      return false;
    }
    return true;
  }
  
  public static boolean isGlobal(EntityType val)
  {
    return val.isGlobal();
  }
  
  public static EntityType createFromInt(int n)
  {
    for (EntityType et : ) {
      if (et.getValue() == n) {
        return et;
      }
    }
    return UNKNOWN;
  }
  
  public static EntityType forObject(Object obj)
  {
    if (obj != null) {
      if ((obj instanceof Integer))
      {
        int v = ((Integer)obj).intValue();
        for (EntityType et : values()) {
          if (et.val == v) {
            return et;
          }
        }
      }
      else if ((obj instanceof String))
      {
        String key = obj.toString();
        for (EntityType et : values()) {
          if (et.name().equalsIgnoreCase(key)) {
            return et;
          }
        }
      }
    }
    return UNKNOWN;
  }
  
  public static EntityType[] orderOfRecompile = { TYPE, STREAM, STREAM_GENERATOR, CACHE, WACTIONSTORE, WINDOW, SOURCE, TARGET, CQ, FLOW, VISUALIZATION, APPLICATION };
}
