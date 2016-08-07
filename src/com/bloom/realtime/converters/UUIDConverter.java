package com.bloom.runtime.converters;

import org.eclipse.persistence.mappings.DatabaseMapping;
import org.eclipse.persistence.mappings.converters.Converter;
import org.eclipse.persistence.sessions.Session;

import com.bloom.uuid.UUID;

public class UUIDConverter
  implements Converter
{
  public static final long serialVersionUID = 2749988897547956681L;
  
  public Object convertObjectValueToDataValue(Object objectValue, Session session)
  {
    if (objectValue == null) {
      return objectValue;
    }
    if ((objectValue instanceof UUID)) {
      return ((UUID)objectValue).toString();
    }
    return null;
  }
  
  public Object convertDataValueToObjectValue(Object dataValue, Session session)
  {
    if (dataValue == null) {
      return dataValue;
    }
    if ((dataValue instanceof String)) {
      if (!((String)dataValue).isEmpty())
      {
        UUID u = new UUID();
        u.setUUIDString((String)dataValue);
        return u;
      }
    }
    return null;
  }
  
  public boolean isMutable()
  {
    return false;
  }
  
  public void initialize(DatabaseMapping databaseMapping, Session session) {}
}
