package com.bloom.kafkamessaging;

import kafka.javaapi.FetchResponse;

public class KafkaException
  extends Exception
{
  FetchResponse fetchResponse;
  
  public KafkaException(String message)
  {
    super(message);
  }
  
  public KafkaException(String message, FetchResponse fetchResponse)
  {
    super(message);
    this.fetchResponse = fetchResponse;
  }
  
  public KafkaException(String message, Throwable cause)
  {
    super(message, cause);
  }
}
