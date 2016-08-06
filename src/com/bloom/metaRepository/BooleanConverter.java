package com.bloom.metaRepository;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.log4j.Logger;
import org.eclipse.persistence.sessions.Session;

public class BooleanConverter
  extends PropertyDefConverter
{
  private static Logger logger = Logger.getLogger(BooleanConverter.class);
  
  TypeReference getTypeReference()
  {
    return new TypeReference() {};
  }
  
  public Object convertDataValueToObjectValue(Object dataValue, Session session)
  {
    if (dataValue == null) {
      return null;
    }
    if ((dataValue != null) && ((dataValue instanceof Boolean))) {
      return dataValue;
    }
    return super.convertDataValueToObjectValue(dataValue, session);
  }
}
