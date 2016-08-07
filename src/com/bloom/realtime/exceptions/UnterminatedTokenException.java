package com.bloom.runtime.exceptions;

import com.bloom.exception.CompilationException;

public class UnterminatedTokenException
  extends CompilationException
{
  private static final long serialVersionUID = 7397745568373236642L;
  public final String term;
  
  public UnterminatedTokenException(String message, String term)
  {
    super(message);
    this.term = term;
  }
}
