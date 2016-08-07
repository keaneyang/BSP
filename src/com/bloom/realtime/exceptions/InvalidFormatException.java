package com.bloom.runtime.exceptions;

import java.io.Serializable;

public class InvalidFormatException
  extends RuntimeException
  implements Serializable
{
  private static final long serialVersionUID = -4976794072771563490L;
  
  public InvalidFormatException() {}
  
  public InvalidFormatException(String message)
  {
    super(message);
  }
  
  public InvalidFormatException(Throwable cause)
  {
    super(cause);
  }
  
  public InvalidFormatException(String message, Throwable cause)
  {
    super(message, cause);
  }
  
  public InvalidFormatException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace)
  {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
