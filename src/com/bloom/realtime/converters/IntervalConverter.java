package com.bloom.runtime.converters;

import org.eclipse.persistence.mappings.DatabaseMapping;
import org.eclipse.persistence.mappings.converters.Converter;
import org.eclipse.persistence.sessions.Session;

import com.bloom.runtime.Interval;

public class IntervalConverter
  implements Converter
{
  private static final long serialVersionUID = -5587994974810478971L;
  
  public Object convertDataValueToObjectValue(Object dataValue, Session arg1)
  {
    if (dataValue == null) {
      return dataValue;
    }
    if ((dataValue instanceof String))
    {
      Long lval = new Long((String)dataValue);
      return new Interval(lval.longValue());
    }
    return null;
  }
  
  public Object convertObjectValueToDataValue(Object iPolicy, Session arg1)
  {
    if (iPolicy == null) {
      return null;
    }
    if ((iPolicy instanceof Interval)) {
      return Long.toString(((Interval)iPolicy).value);
    }
    return null;
  }
  
  public void initialize(DatabaseMapping arg0, Session arg1) {}
  
  public boolean isMutable()
  {
    return false;
  }
}
