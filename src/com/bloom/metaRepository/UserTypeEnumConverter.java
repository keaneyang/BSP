package com.bloom.metaRepository;

import org.eclipse.persistence.mappings.DatabaseMapping;
import org.eclipse.persistence.mappings.converters.Converter;
import org.eclipse.persistence.sessions.Session;

import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.User.AUTHORIZATION_TYPE;

public class UserTypeEnumConverter
  implements Converter
{
  private static final long serialVersionUID = 5863685664514502453L;
  
  public Object convertObjectValueToDataValue(Object objectValue, Session session)
  {
    if ((objectValue instanceof MetaInfo.User.AUTHORIZATION_TYPE)) {
      return ((MetaInfo.User.AUTHORIZATION_TYPE)objectValue).toString();
    }
    return null;
  }
  
  public Object convertDataValueToObjectValue(Object dataValue, Session session)
  {
    if (dataValue == null) {
      return dataValue;
    }
    if ((dataValue instanceof String))
    {
      if (!((String)dataValue).isEmpty())
      {
        if (((String)dataValue).equalsIgnoreCase(MetaInfo.User.AUTHORIZATION_TYPE.LDAP.toString())) {
          return MetaInfo.User.AUTHORIZATION_TYPE.LDAP;
        }
        return MetaInfo.User.AUTHORIZATION_TYPE.INTERNAL;
      }
    }
    else if ((dataValue instanceof MetaInfo.User.AUTHORIZATION_TYPE)) {
      return dataValue;
    }
    return MetaInfo.User.AUTHORIZATION_TYPE.INTERNAL;
  }
  
  public boolean isMutable()
  {
    return false;
  }
  
  public void initialize(DatabaseMapping mapping, Session session) {}
}
