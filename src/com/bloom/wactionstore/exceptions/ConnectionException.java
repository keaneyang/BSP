package com.bloom.wactionstore.exceptions;

public class ConnectionException
  extends WActionStoreException
{
  private static final long serialVersionUID = -6132114902661917869L;
  private static final String MSG_EXC_MESSAGE = "Provider '%s' cannot connect to instance '%s': %s";
  public final Exception providerException;
  
  public ConnectionException(String providerName, String instanceName, Exception providerException)
  {
    super(String.format("Provider '%s' cannot connect to instance '%s': %s", new Object[] { providerName, instanceName, providerException.getMessage() }));
    this.providerException = providerException;
  }
}
