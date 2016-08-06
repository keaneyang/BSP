package com.bloom.metaRepository;

public class MetaDataRepositoryException
  extends Exception
{
  public MetaDataRepositoryException(String s)
  {
    super(s);
  }
  
  public MetaDataRepositoryException(String msg, Throwable ex)
  {
    super(msg, ex);
  }
}
