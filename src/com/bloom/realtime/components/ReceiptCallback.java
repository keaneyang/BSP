package com.bloom.runtime.components;

import com.bloom.recovery.Position;

public abstract interface ReceiptCallback
{
  public abstract void ack(Position paramPosition);
}

