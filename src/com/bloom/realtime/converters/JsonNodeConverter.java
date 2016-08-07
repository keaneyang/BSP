package com.bloom.runtime.converters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.bloom.event.ObjectMapperFactory;
import org.apache.log4j.Logger;
import org.eclipse.persistence.internal.helper.DatabaseField;
import org.eclipse.persistence.mappings.DatabaseMapping;
import org.eclipse.persistence.mappings.converters.Converter;
import org.eclipse.persistence.sessions.Session;

public class JsonNodeConverter
  implements Converter
{
  private static Logger logger = Logger.getLogger(JsonNodeConverter.class);
  private static final long serialVersionUID = -1814149577596807517L;
  private final ObjectMapper jsonMapper = ObjectMapperFactory.newInstance();
  
  public Object convertObjectValueToDataValue(Object objectValue, Session session)
  {
    if (objectValue == null) {
      return objectValue;
    }
    if ((objectValue instanceof JsonNode)) {
      return ((JsonNode)objectValue).toString();
    }
    return null;
  }
  
  public Object convertDataValueToObjectValue(Object dataValue, Session session)
  {
    if (dataValue == null) {
      return dataValue;
    }
    if (((dataValue instanceof String)) && 
      (!((String)dataValue).isEmpty())) {
      try
      {
        return this.jsonMapper.readTree((String)dataValue);
      }
      catch (Exception ex)
      {
        logger.error("error building jsonnode from string : " + (String)dataValue);
      }
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
      mapping.getField().setType(String.class);
      mapping.getField().setSqlType(12);
      mapping.getField().setLength(6500);
    }
  }
}
