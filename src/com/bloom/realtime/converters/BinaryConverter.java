package com.bloom.runtime.converters;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.eclipse.persistence.mappings.DatabaseMapping;
import org.eclipse.persistence.mappings.converters.Converter;
import org.eclipse.persistence.sessions.Session;

public class BinaryConverter
  implements Converter
{
  private static final long serialVersionUID = -6537432266455948270L;
  
  public Object convertObjectValueToDataValue(Object objectValue, Session session)
  {
    if (objectValue != null) {
      try
      {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(objectValue);
        oos.close();
        return bos.toByteArray();
      }
      catch (IOException e) {}
    }
    return null;
  }
  
  public Object convertDataValueToObjectValue(Object dataValue, Session session)
  {
    if ((dataValue instanceof byte[])) {
      try
      {
        ByteArrayInputStream bis = new ByteArrayInputStream((byte[])dataValue);
        ObjectInputStream ois = new ObjectInputStream(bis);
        return ois.readObject();
      }
      catch (IOException|ClassNotFoundException e) {}
    }
    return null;
  }
  
  public boolean isMutable()
  {
    return false;
  }
  
  public void initialize(DatabaseMapping databaseMapping, Session session) {}
}
