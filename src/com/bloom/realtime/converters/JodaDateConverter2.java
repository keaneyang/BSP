package com.bloom.runtime.converters;

import java.util.Date;
import org.eclipse.persistence.internal.helper.DatabaseField;
import org.eclipse.persistence.mappings.DatabaseMapping;
import org.eclipse.persistence.mappings.converters.Converter;
import org.eclipse.persistence.sessions.Session;
import org.joda.time.DateTime;

public class JodaDateConverter2
  implements Converter
{
  private static final long serialVersionUID = -4622230539751930045L;
  
  public Object convertObjectValueToDataValue(Object objectValue, Session session)
  {
    if (objectValue == null) {
      return objectValue;
    }
    if ((objectValue instanceof DateTime)) {
      return ((DateTime)objectValue).toDate();
    }
    return null;
  }
  
  public Object convertDataValueToObjectValue(Object dataValue, Session session)
  {
    if (dataValue == null) {
      return dataValue;
    }
    if ((dataValue instanceof Date)) {
      return new DateTime((Date)dataValue);
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
      mapping.getField().setType(Date.class);
      mapping.getField().setSqlType(93);
    }
  }
}
