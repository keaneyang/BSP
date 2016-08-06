package com.bloom.persistence;

public class ElasticSearchSchemaException
  extends Exception
{
  private String message;
  
  public ElasticSearchSchemaException(String message)
  {
    super(message);
  }
}
