package com.bloom.recovery;

import org.eclipse.persistence.mappings.DatabaseMapping;
import org.eclipse.persistence.mappings.converters.Converter;
import org.eclipse.persistence.sessions.Session;

import com.bloom.recovery.Path;

public class PathItemListConverter
  implements Converter
{
  public static final long serialVersionUID = 2749988897547956681L;
  
  public Object convertObjectValueToDataValue(Object objectValue, Session session)
  {
    if ((objectValue instanceof Path.ItemList)) {
      return ((Path.ItemList)objectValue).toString();
    }
    return null;
  }
  
  public Object convertDataValueToObjectValue(Object dataValue, Session session)
  {
    if ((dataValue instanceof String)) {
      return Path.ItemList.fromString((String)dataValue);
    }
    return null;
  }
  
  public boolean isMutable()
  {
    return false;
  }
  
  public void initialize(DatabaseMapping databaseMapping, Session session) {}
}
