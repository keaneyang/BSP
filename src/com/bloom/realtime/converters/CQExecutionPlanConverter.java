package com.bloom.runtime.converters;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.eclipse.persistence.mappings.DatabaseMapping;
import org.eclipse.persistence.mappings.converters.Converter;
import org.eclipse.persistence.sessions.Session;

import com.bloom.runtime.meta.CQExecutionPlan;

public class CQExecutionPlanConverter
  implements Converter
{
  public static final long serialVersionUID = 2740988897548956781L;
  
  public Object convertObjectValueToDataValue(Object objectValue, Session session)
  {
    if (objectValue == null) {
      return objectValue;
    }
    if ((objectValue instanceof CQExecutionPlan))
    {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      byte[] b;
      try
      {
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        
        oos.writeObject(objectValue);
        oos.close();
        b = bos.toByteArray();
      }
      catch (IOException e)
      {
        b = null;
      }
      return b;
    }
    return null;
  }
  
  public Object convertDataValueToObjectValue(Object dataValue, Session session)
  {
    if (dataValue == null) {
      return dataValue;
    }
    if ((dataValue instanceof byte[]))
    {
      CQExecutionPlan cqp = null;
      ByteArrayInputStream bis = new ByteArrayInputStream((byte[])dataValue);
      try
      {
        ObjectInputStream ois = new ObjectInputStream(bis);
        cqp = (CQExecutionPlan)ois.readObject();
      }
      catch (ClassNotFoundException e)
      {
        cqp = null;
      }
      catch (IOException e)
      {
        cqp = null;
      }
      return cqp;
    }
    return null;
  }
  
  public boolean isMutable()
  {
    return false;
  }
  
  public void initialize(DatabaseMapping databaseMapping, Session session) {}
}
