package com.bloom.runtime.compiler.stmts;

import java.util.List;

import com.bloom.runtime.Property;
import com.bloom.runtime.meta.MetaInfo.User.AUTHORIZATION_TYPE;

public class UserProperty
{
  public List<String> lroles;
  String uname;
  public String defaultnamespace;
  public List<Property> properties;
  public MetaInfo.User.AUTHORIZATION_TYPE originType = MetaInfo.User.AUTHORIZATION_TYPE.INTERNAL;
  public String ldap;
  
  public UserProperty(List<String> lroles, String defaultnamespace, List<Property> props)
  {
    this.lroles = lroles;
    this.defaultnamespace = defaultnamespace;
    this.properties = props;
  }
  
  public void setUname(String uname)
  {
    this.uname = uname;
  }
}
