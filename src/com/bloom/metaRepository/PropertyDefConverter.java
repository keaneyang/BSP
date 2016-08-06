package com.bloom.metaRepository;

import com.bloom.runtime.converters.MapConverter;
import com.bloom.runtime.meta.MetaInfo.PropertyDef;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.bloom.event.ObjectMapperFactory;

import java.io.IOException;
import java.util.Map;
import org.apache.log4j.Logger;
import org.eclipse.persistence.mappings.DatabaseMapping;
import org.eclipse.persistence.mappings.converters.Converter;
import org.eclipse.persistence.sessions.Session;

public class PropertyDefConverter
  implements Converter
{
  private static Logger logger = Logger.getLogger(MapConverter.class);
  public static final long serialVersionUID = 2740988897547957681L;
  private static ObjectMapper mapper = ObjectMapperFactory.newInstance();
  
  public Object convertObjectValueToDataValue(Object objectValue, Session session)
  {
    if (objectValue == null) {
      return objectValue;
    }
    try
    {
      String json = mapper.writeValueAsString(objectValue);
      if (logger.isTraceEnabled()) {
        logger.trace("Converted property value " + objectValue + " to " + json);
      }
      return json;
    }
    catch (IOException e)
    {
      logger.error(e);
    }
    return null;
  }
  
  TypeReference getTypeReference()
  {
    return new TypeReference() {};
  }
  
  public Object convertDataValueToObjectValue(Object dataValue, Session session)
  {
    if (dataValue == null) {
      return dataValue;
    }
    if (((dataValue instanceof String)) && (((String)dataValue).trim().length() != 0)) {
      try
      {
        return mapper.readValue((String)dataValue, getTypeReference());
      }
      catch (IOException e)
      {
        logger.error("Problem converting object: " + dataValue, e);
        return null;
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
