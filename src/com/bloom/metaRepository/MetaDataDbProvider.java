package com.bloom.metaRepository;

public abstract interface MetaDataDbProvider
{
  public abstract void setJDBCDriver();
  
  public abstract String getJDBCURL(String paramString1, String paramString2, String paramString3, String paramString4);
  
  public abstract String getJDBCDriver();
  
  public abstract Object getIntegerValueFromData(Object paramObject);
  
  public abstract String askMetaDataRepositoryLocation(String paramString);
  
  public abstract String askDefaultDBLocationAppendedWithPort(String paramString);
  
  public abstract String askDBUserName(String paramString);
  
  public abstract String askDBPassword(String paramString);
}

