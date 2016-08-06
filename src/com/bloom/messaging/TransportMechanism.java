package com.bloom.messaging;

public enum TransportMechanism
{
  INPROC(1),  IPC(2),  TCP(3);
  
  private final int val;
  
  private TransportMechanism(int val)
  {
    this.val = val;
  }
}
