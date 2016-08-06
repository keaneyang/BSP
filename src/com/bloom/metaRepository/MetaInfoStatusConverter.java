package com.bloom.metaRepository;

import flexjson.JSONDeserializer;
import flexjson.JSONSerializer;
import org.apache.log4j.Logger;
import org.eclipse.persistence.mappings.DatabaseMapping;
import org.eclipse.persistence.mappings.converters.Converter;
import org.eclipse.persistence.sessions.Session;

import com.bloom.runtime.meta.MetaInfoStatus;

public class MetaInfoStatusConverter
  implements Converter
{
  private static Logger logger = Logger.getLogger(MetaInfoStatusConverter.class);
  JSONSerializer ser = new JSONSerializer();
  JSONDeserializer deser = new JSONDeserializer();
  private static final long serialVersionUID = 1061473287059512670L;
  
  public Object convertObjectValueToDataValue(Object objectValue, Session session)
  {
    if (objectValue == null) {
      return this.ser.deepSerialize(new MetaInfoStatus());
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
      return new MetaInfoStatus();
    }
    try
    {
      return this.deser.deserialize((String)dataValue);
    }
    catch (Exception e)
    {
      logger.error("Problem deserializing", e);
    }
    return null;
  }
  
  public boolean isMutable()
  {
    return false;
  }
  
  public void initialize(DatabaseMapping mapping, Session session) {}
}
