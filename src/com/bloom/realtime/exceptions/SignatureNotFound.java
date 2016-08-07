package com.bloom.runtime.exceptions;

public class SignatureNotFound
  extends Exception
{
  private static final long serialVersionUID = 4606993564528798750L;
  
  public SignatureNotFound() {}
  
  public SignatureNotFound(String message)
  {
    super(message);
  }
}
