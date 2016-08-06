package com.bloom.wactionstore.exceptions;

public class WActionStoreMissingException
  extends WActionStoreException
{
  private static final long serialVersionUID = -5031853353405227519L;
  private static final String MSG_EXC_MESSAGE = "WActionStore '%s' cannot be found by provider '%s', instance '%s'";
  public final String wActionStoreName;
  
  public WActionStoreMissingException(String providerName, String instanceName, String wActionStoreName)
  {
    super(String.format("WActionStore '%s' cannot be found by provider '%s', instance '%s'", new Object[] { wActionStoreName, providerName, instanceName }));
    this.wActionStoreName = wActionStoreName;
  }
}
