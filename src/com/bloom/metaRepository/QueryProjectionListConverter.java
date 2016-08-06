package com.bloom.metaRepository;

import com.bloom.intf.QueryManager.QueryProjection;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.bloom.event.ObjectMapperFactory;

import java.io.IOException;
import java.util.List;
import org.apache.log4j.Logger;
import org.eclipse.persistence.sessions.Session;

public class QueryProjectionListConverter
  extends PropertyDefConverter
{
  private static Logger logger = Logger.getLogger(QueryProjectionListConverter.class);
  private static final long serialVersionUID = -8400939726819076422L;
  
  TypeReference getTypeReference()
  {
    return new TypeReference() {};
  }
  
  public Object convertObjectValueToDataValue(Object objectValue, Session session)
  {
    if (objectValue == null) {
      return objectValue;
    }
    ObjectMapper mapper = ObjectMapperFactory.newInstance();
    try
    {
      String json = mapper.writeValueAsString(objectValue);
      if (logger.isTraceEnabled()) {
        logger.trace("Converted rsfield " + objectValue + " to " + json);
      }
      return json;
    }
    catch (IOException e)
    {
      logger.error(e);
    }
    return null;
  }
  
  public Object convertDataValueToObjectValue(Object dataValue, Session session)
  {
    if (dataValue == null) {
      return dataValue;
    }
    if ((dataValue instanceof String)) {
      try
      {
        ObjectMapper mapper = ObjectMapperFactory.newInstance();
        return mapper.readValue((String)dataValue, getTypeReference());
      }
      catch (IOException e)
      {
        logger.error(e);
        return null;
      }
    }
    return null;
  }
}
