package com.bloom.metaRepository;

import com.fasterxml.jackson.core.type.TypeReference;
import org.eclipse.persistence.sessions.Session;

public class NullToEmptyStringConverter
  extends PropertyDefConverter
{
  public Object convertDataValueToObjectValue(Object dataValue, Session session)
  {
    if (dataValue == null) {
      return "";
    }
    return super.convertDataValueToObjectValue(dataValue, session);
  }
  
  TypeReference getTypeReference()
  {
    return new TypeReference() {};
  }
}
