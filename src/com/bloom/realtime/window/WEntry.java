package com.bloom.runtime.window;

import com.bloom.runtime.containers.WAEvent;

public class WEntry
{
  final WAEvent data;
  final long id;
  final long timestamp;
  
  WEntry(WAEvent data, long id, long timestamp)
  {
    this.data = data;
    this.id = id;
    this.timestamp = timestamp;
  }
  
  public String toString()
  {
    return "" + this.data + "<" + this.id + ">";
  }
}
