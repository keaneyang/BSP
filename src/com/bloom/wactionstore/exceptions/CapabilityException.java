package com.bloom.wactionstore.exceptions;

public class CapabilityException
  extends WActionStoreException
{
  private static final long serialVersionUID = -6132114902661917869L;
  private static final String MSG_EXC_MESSAGE = "Capability '%s' not provided by WActionStore provider '%s'";
  public final String capabilityViolated;
  
  public CapabilityException(String providerName, String capabilityViolated)
  {
    super(String.format("Capability '%s' not provided by WActionStore provider '%s'", new Object[] { capabilityViolated, providerName }));
    this.capabilityViolated = capabilityViolated;
  }
  
  public CapabilityException(String capabilityViolated)
  {
    this("<Unknown>", capabilityViolated);
  }
}
