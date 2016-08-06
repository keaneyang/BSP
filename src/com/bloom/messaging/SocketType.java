package com.bloom.messaging;

public enum SocketType
{
  PUB(1),  SUB(2),  SYNCREP(3),  ASYNCREP(3),  SYNCREQ(4),  ASYNCREQ(5),  PUSH(6),  PULL(7);
  
  private final int val;
  
  private SocketType(int i)
  {
    this.val = i;
  }
}
