package com.bloom.metaRepository;

import org.apache.log4j.Logger;
import org.eclipse.persistence.mappings.DatabaseMapping;
import org.eclipse.persistence.mappings.converters.Converter;
import org.eclipse.persistence.sessions.Session;

import com.bloom.runtime.Server;
import com.bloom.runtime.components.EntityType;

public class EntityTypeEnumConverter
  implements Converter
{
  private static final long serialVersionUID = 2989187146445709055L;
  private static Logger logger = Logger.getLogger(EntityTypeEnumConverter.class);
  
  public Object convertObjectValueToDataValue(Object objectValue, Session session)
  {
    if ((objectValue instanceof EntityType)) {
      return Integer.valueOf(((EntityType)objectValue).getValue());
    }
    return Integer.valueOf(EntityType.UNKNOWN.getValue());
  }
  
  public Object convertDataValueToObjectValue(Object dataValue, Session session)
  {
    MetaDataDbProvider metaDataDbProvider = Server.getMetaDataDBProviderDetails();
    return metaDataDbProvider.getIntegerValueFromData(dataValue);
  }
  
  public boolean isMutable()
  {
    return false;
  }
  
  public void initialize(DatabaseMapping mapping, Session session) {}
}
