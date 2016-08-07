package com.bloom.runtime.converters;

import flexjson.JSONDeserializer;
import flexjson.JSONSerializer;
import org.apache.log4j.Logger;
import org.eclipse.persistence.mappings.DatabaseMapping;
import org.eclipse.persistence.mappings.converters.Converter;
import org.eclipse.persistence.sessions.Session;

public class MapConverter
  implements Converter
{
  private static Logger logger = Logger.getLogger(MapConverter.class);
  public static final long serialVersionUID = 2740988897547957681L;
  JSONSerializer ser = new JSONSerializer();
  JSONDeserializer deser = new JSONDeserializer();
  
  public Object convertObjectValueToDataValue(Object objectValue, Session session)
  {
    if (objectValue == null) {
      return objectValue;
    }
    synchronized (this.ser)
    {
      try
      {
        return this.ser.deepSerialize(objectValue);
      }
      catch (Exception e)
      {
        logger.error("Problem serializing", e);
        return null;
      }
    }
  }
  
  public Object convertDataValueToObjectValue(Object dataValue, Session session)
  {
    if (dataValue == null) {
      return dataValue;
    }
    if ((dataValue instanceof String)) {
      try
      {
        return this.deser.deserialize((String)dataValue);
      }
      catch (Exception e)
      {
        logger.error("Problem deserializing", e);
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
