package com.bloom.runtime.converters;

import org.eclipse.persistence.internal.helper.DatabaseField;
import org.eclipse.persistence.mappings.DatabaseMapping;
import org.eclipse.persistence.mappings.converters.Converter;
import org.eclipse.persistence.sessions.Session;
import org.joda.time.DateTime;

public class JodaDateConverter
  implements Converter
{
  private static final long serialVersionUID = -7080852479188785017L;
  
  public Object convertObjectValueToDataValue(Object objectValue, Session session)
  {
    if (objectValue == null) {
      return objectValue;
    }
    if ((objectValue instanceof DateTime)) {
      return Long.valueOf(((DateTime)objectValue).getMillis());
    }
    return null;
  }
  
  public Object convertDataValueToObjectValue(Object dataValue, Session session)
  {
    if (dataValue == null) {
      return dataValue;
    }
    if ((dataValue instanceof String))
    {
      if (!((String)dataValue).isEmpty())
      {
        long l = Long.parseLong((String)dataValue);
        return new DateTime(l);
      }
    }
    else if ((dataValue instanceof Long)) {
      return new DateTime(((Long)dataValue).longValue());
    }
    return null;
  }
  
  public boolean isMutable()
  {
    return false;
  }
  
  public void initialize(DatabaseMapping mapping, Session session)
  {
    if (mapping != null)
    {
      mapping.getField().setType(Long.class);
      mapping.getField().setSqlType(-5);
    }
  }
}
