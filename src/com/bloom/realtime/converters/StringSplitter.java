package com.bloom.runtime.converters;

import java.util.ArrayList;
import java.util.List;
import org.eclipse.persistence.mappings.DatabaseMapping;
import org.eclipse.persistence.mappings.converters.Converter;
import org.eclipse.persistence.sessions.Session;

public class StringSplitter
  implements Converter
{
  private static final long serialVersionUID = -3662523612151079504L;
  
  public Object convertDataValueToObjectValue(Object dataval, Session arg1)
  {
    if ((dataval instanceof String))
    {
      List<String> slist = new ArrayList();
      
      String[] sval = ((String)dataval).split(",");
      for (int i = 0; i < sval.length; i++) {
        slist.add(sval[i]);
      }
      return slist;
    }
    return null;
  }
  
  public Object convertObjectValueToDataValue(Object slist, Session arg1)
  {
    if ((slist instanceof List))
    {
      String sval = "";
      List<String> strlist = (List)slist;
      for (int i = 0; i < strlist.size(); i++)
      {
        sval = sval + (String)strlist.get(i);
        if (i + 1 != strlist.size()) {
          sval = sval + ",";
        }
      }
      return sval;
    }
    return null;
  }
  
  public void initialize(DatabaseMapping arg0, Session arg1) {}
  
  public boolean isMutable()
  {
    return false;
  }
}
