package com.bloom.messaging;

public abstract interface RequestHandler
{
  public abstract Object handleRequest(Object paramObject)
    throws Exception;
}

