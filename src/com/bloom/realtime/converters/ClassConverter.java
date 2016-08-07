package com.bloom.runtime.converters;

import org.apache.log4j.Logger;
import org.eclipse.persistence.mappings.DatabaseMapping;
import org.eclipse.persistence.mappings.converters.Converter;
import org.eclipse.persistence.sessions.Session;

public class ClassConverter
  implements Converter
{
  private static final long serialVersionUID = 6948468434440252794L;
  private static Logger logger = Logger.getLogger(ClassConverter.class);
  
  public Object convertDataValueToObjectValue(Object dataValue, Session arg1)
  {
    if (dataValue == null) {
      return dataValue;
    }
    if ((dataValue instanceof String)) {
      try
      {
        return Class.forName((String)dataValue);
      }
      catch (ClassNotFoundException e)
      {
        logger.error(e);
        return null;
      }
    }
    return null;
  }
  
  public Object convertObjectValueToDataValue(Object classname, Session arg1)
  {
    if (classname == null) {
      return null;
    }
    if ((classname instanceof Class)) {
      return ((Class)classname).getName();
    }
    return null;
  }
  
  public void initialize(DatabaseMapping arg0, Session arg1) {}
  
  public boolean isMutable()
  {
    return false;
  }
}
