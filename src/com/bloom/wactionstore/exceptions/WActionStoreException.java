package com.bloom.wactionstore.exceptions;

public class WActionStoreException
  extends RuntimeException
{
  private static final long serialVersionUID = 6924180541153454102L;
  
  public WActionStoreException(String message)
  {
    super(message);
  }
  
  public WActionStoreException(String message, Throwable cause)
  {
    super(message, cause);
  }
}
