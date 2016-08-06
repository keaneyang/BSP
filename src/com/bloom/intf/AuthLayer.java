package com.bloom.intf;

import java.util.Map;

import com.bloom.security.Password;

public abstract interface AuthLayer
{
  public abstract void setup(Map<String, Object> paramMap);
  
  public abstract void connect();
  
  public abstract boolean find(String paramString);
  
  public abstract boolean getAccess(String paramString, Password paramPassword);
  
  public abstract AuthType getType();
  
  public abstract void close();
  
  public static enum AuthType
  {
    LDAP,  AD;
    
    private AuthType() {}
  }
}
